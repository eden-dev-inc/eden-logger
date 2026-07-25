//! Thread-local producer lanes drained by the shared exporter worker.

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use eden_logger::{EdenLog, RequestFields};
use parking_lot::Mutex;

use crate::{Shared, decrement};

const DRAIN_QUANTUM: usize = 64;
static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// Producer lanes already registered by this thread.
    ///
    /// The type-erased entries allow applications to install sinks for more
    /// than one RequestFields type while keeping the steady-state lookup and
    /// queue mutation thread-local.
    static LOCAL_PRODUCERS: RefCell<Vec<(u64, Box<dyn Any>)>> = const { RefCell::new(Vec::new()) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueKind {
    Normal,
    Reserved,
}

pub(crate) struct QueuedLog<R: RequestFields> {
    pub log: EdenLog<R>,
    pub queue: QueueKind,
}

pub(crate) enum SubmitResult {
    Accepted,
    Full,
}

pub(crate) trait CollectorControl: Send + Sync {
    fn clear(&self);
}

struct LaneQueues<R: RequestFields> {
    normal: VecDeque<EdenLog<R>>,
    reserved: VecDeque<EdenLog<R>>,
}

impl<R: RequestFields> LaneQueues<R> {
    fn new() -> Self {
        Self { normal: VecDeque::new(), reserved: VecDeque::new() }
    }

    fn is_empty(&self) -> bool {
        self.normal.is_empty() && self.reserved.is_empty()
    }
}

struct ProducerLane<R: RequestFields> {
    queues: Mutex<LaneQueues<R>>,
    worker_spare: Mutex<Option<LaneQueues<R>>>,
    closed: AtomicBool,
}

impl<R: RequestFields> ProducerLane<R> {
    fn new() -> Self {
        Self {
            queues: Mutex::new(LaneQueues::new()),
            worker_spare: Mutex::new(None),
            closed: AtomicBool::new(false),
        }
    }

    fn push(&self, log: EdenLog<R>, queue: QueueKind) -> bool {
        let mut queues = self.queues.lock();
        let was_empty = queues.is_empty();
        match queue {
            QueueKind::Normal => queues.normal.push_back(log),
            QueueKind::Reserved => queues.reserved.push_back(log),
        }
        was_empty
    }

    fn is_closed_and_empty(&self) -> bool {
        self.closed.load(Ordering::Acquire) && self.queues.lock().is_empty()
    }

    fn take(&self) -> LaneQueues<R> {
        let replacement = self.worker_spare.lock().take().unwrap_or_else(LaneQueues::new);
        let mut queues = self.queues.lock();
        std::mem::replace(&mut *queues, replacement)
    }

    /// Return an emptied queue pair for the next producer/worker swap.
    ///
    /// This preserves the backing allocations across drains. Producers can
    /// continue filling `active` while the worker drains the detached pair.
    fn recycle(&self, queues: LaneQueues<R>) {
        debug_assert!(queues.is_empty());
        let mut spare = self.worker_spare.lock();
        if spare.is_none() {
            *spare = Some(queues);
        }
    }
}

struct LocalProducer<R: RequestFields> {
    lane: Arc<ProducerLane<R>>,
    shared: Arc<Shared>,
}

impl<R: RequestFields> Drop for LocalProducer<R> {
    fn drop(&mut self) {
        self.lane.closed.store(true, Ordering::Release);
        self.shared.records.notify_one();
    }
}

struct LaneRegistry<R: RequestFields> {
    lanes: Vec<Arc<ProducerLane<R>>>,
    cursor: usize,
}

/// Bounded collector with one registered queue lane per producing thread.
pub(crate) struct LogCollector<R: RequestFields> {
    id: u64,
    normal_capacity: u64,
    reserved_capacity: u64,
    lanes: Mutex<LaneRegistry<R>>,
    shared: Arc<Shared>,
}

impl<R: RequestFields> LogCollector<R> {
    pub fn new(normal_capacity: usize, reserved_capacity: usize, shared: Arc<Shared>) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_COLLECTOR_ID.fetch_add(1, Ordering::Relaxed),
            normal_capacity: normal_capacity.try_into().unwrap_or(u64::MAX),
            reserved_capacity: reserved_capacity.try_into().unwrap_or(u64::MAX),
            lanes: Mutex::new(LaneRegistry { lanes: Vec::new(), cursor: 0 }),
            shared,
        })
    }

    /// Submit through this thread's registered lane.
    #[inline]
    pub fn submit(self: &Arc<Self>, log: EdenLog<R>, priority: bool) -> SubmitResult {
        LOCAL_PRODUCERS.with_borrow_mut(|entries| {
            let position = entries.iter().position(|(id, _)| *id == self.id).unwrap_or_else(|| {
                let lane = self.register_lane();
                entries.push((self.id, Box::new(LocalProducer::<R> { lane, shared: Arc::clone(&self.shared) })));
                entries.len() - 1
            });
            let producer =
                entries[position].1.downcast_mut::<LocalProducer<R>>().expect("collector IDs are unique across RequestFields types");
            self.submit_to_lane(&producer.lane, log, priority)
        })
    }

    fn register_lane(&self) -> Arc<ProducerLane<R>> {
        let lane = Arc::new(ProducerLane::new());
        self.lanes.lock().lanes.push(Arc::clone(&lane));
        self.shared.metrics.producer_lanes.fetch_add(1, Ordering::Relaxed);
        lane
    }

    fn submit_to_lane(&self, lane: &ProducerLane<R>, log: EdenLog<R>, priority: bool) -> SubmitResult {
        let (queue, log) = if claim(&self.shared.metrics.normal_queue_depth, self.normal_capacity) {
            (QueueKind::Normal, log)
        } else if priority && claim(&self.shared.metrics.reserved_queue_depth, self.reserved_capacity) {
            (QueueKind::Reserved, log)
        } else {
            return SubmitResult::Full;
        };

        if lane.push(log, queue) {
            self.shared.records.notify_one();
        }
        SubmitResult::Accepted
    }

    /// Move currently available records into the worker-owned inbox.
    ///
    /// Reserved records are drained first. Each lane contributes at most one
    /// quantum per pass so a single noisy producer cannot starve other threads.
    pub fn drain_into(&self, reserved_output: &mut VecDeque<QueuedLog<R>>, normal_output: &mut VecDeque<QueuedLog<R>>) -> usize {
        let start_len = reserved_output.len().saturating_add(normal_output.len());
        let lanes = {
            let mut registry = self.lanes.lock();
            let before = registry.lanes.len();
            registry.lanes.retain(|lane| !lane.is_closed_and_empty());
            let removed = before - registry.lanes.len();
            if removed > 0 {
                self.shared.metrics.producer_lanes.fetch_sub(removed as u64, Ordering::Relaxed);
            }
            let lane_count = registry.lanes.len();
            if lane_count == 0 {
                registry.cursor = 0;
                return 0;
            }
            let start = registry.cursor % lane_count;
            let ordered = (0..lane_count).map(|offset| Arc::clone(&registry.lanes[(start + offset) % lane_count])).collect::<Vec<_>>();
            registry.cursor = (start + 1) % lane_count;
            ordered
        };

        // Swap each producer's queues out under a very short critical section.
        // All record movement and fairness work happens after producer locks
        // have been released.
        let mut batches = lanes.iter().map(|lane| lane.take()).collect::<Vec<_>>();
        drain_batches(&mut batches, QueueKind::Reserved, reserved_output);
        drain_batches(&mut batches, QueueKind::Normal, normal_output);
        for (lane, queues) in lanes.iter().zip(batches) {
            lane.recycle(queues);
        }
        reserved_output.len().saturating_add(normal_output.len()).saturating_sub(start_len)
    }

    /// Release one global capacity slot after the worker takes ownership.
    pub fn release(&self, queue: QueueKind) {
        match queue {
            QueueKind::Normal => decrement(&self.shared.metrics.normal_queue_depth),
            QueueKind::Reserved => decrement(&self.shared.metrics.reserved_queue_depth),
        }
    }

    #[cfg(test)]
    pub fn registered_lanes(&self) -> usize {
        self.lanes.lock().lanes.len()
    }
}

impl<R: RequestFields> CollectorControl for LogCollector<R> {
    fn clear(&self) {
        let mut registry = self.lanes.lock();
        for lane in &registry.lanes {
            let mut queues = lane.queues.lock();
            queues.normal.clear();
            queues.reserved.clear();
            if let Some(spare) = &mut *lane.worker_spare.lock() {
                spare.normal.clear();
                spare.reserved.clear();
            }
        }
        registry.lanes.clear();
        registry.cursor = 0;
        self.shared.metrics.producer_lanes.store(0, Ordering::Relaxed);
    }
}

fn claim(depth: &AtomicU64, capacity: u64) -> bool {
    let previous = depth.fetch_add(1, Ordering::AcqRel);
    if previous < capacity {
        true
    } else {
        depth.fetch_sub(1, Ordering::Release);
        false
    }
}

fn drain_batches<R: RequestFields>(batches: &mut [LaneQueues<R>], queue: QueueKind, output: &mut VecDeque<QueuedLog<R>>) {
    loop {
        let mut drained = 0;
        for batch in batches.iter_mut() {
            let source = match queue {
                QueueKind::Normal => &mut batch.normal,
                QueueKind::Reserved => &mut batch.reserved,
            };
            let take = DRAIN_QUANTUM.min(source.len());
            drained += take;
            output.extend(source.drain(..take).map(|log| QueuedLog { log, queue }));
        }
        if drained == 0 {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use eden_logger::{FieldWriter, LogAudience, LogContext, LogLevel};
    use hegel::TestCase;
    use hegel::generators as gs;
    use tokio::sync::Notify;

    use super::*;
    use crate::metrics::ExporterMetrics;

    #[derive(Clone, Default)]
    struct TestFields;

    impl RequestFields for TestFields {
        fn write_display(&self, _: &mut dyn FieldWriter) {}
        fn write_json(&self, _: &mut dyn FieldWriter) {}
        fn merge(&mut self, _: Self) {}
    }

    fn shared() -> Arc<Shared> {
        Arc::new(Shared {
            accepting: AtomicBool::new(true),
            metrics: ExporterMetrics::default(),
            shutdown: Notify::new(),
            records: Notify::new(),
            diagnostic_interval_millis: Duration::from_secs(60).as_millis() as u64,
            last_diagnostic_millis: AtomicU64::new(0),
            last_error: std::sync::Mutex::new(None),
        })
    }

    fn log(message: &str) -> EdenLog<TestFields> {
        EdenLog::new(LogLevel::Info, message, &LogContext::<TestFields>::new(), LogAudience::Internal)
    }

    #[test]
    fn recycles_queue_allocations_between_worker_drains() {
        let lane = ProducerLane::new();
        for _ in 0..128 {
            lane.push(log("buffered"), QueueKind::Normal);
        }

        let mut drained = lane.take();
        let grown_capacity = drained.normal.capacity();
        assert!(grown_capacity >= 128);
        drained.normal.clear();
        lane.recycle(drained);

        lane.push(log("next"), QueueKind::Normal);
        let mut next = lane.take();
        assert_eq!(next.normal.len(), 1);
        next.normal.clear();
        lane.recycle(next);

        assert!(lane.queues.lock().normal.capacity() >= grown_capacity);
    }

    #[test]
    fn registers_one_lane_per_thread_and_drains_all_records() {
        let shared = shared();
        let collector = LogCollector::new(8, 2, Arc::clone(&shared));
        assert!(matches!(collector.submit(log("main-1"), false), SubmitResult::Accepted));
        assert!(matches!(collector.submit(log("main-2"), false), SubmitResult::Accepted));
        assert_eq!(collector.registered_lanes(), 1);

        let thread_collector = Arc::clone(&collector);
        std::thread::spawn(move || {
            assert!(matches!(thread_collector.submit(log("thread"), false), SubmitResult::Accepted));
        })
        .join()
        .expect("producer thread");
        assert_eq!(collector.registered_lanes(), 2);
        assert_eq!(shared.metrics.producer_lanes.load(Ordering::Relaxed), 2);

        let mut reserved = VecDeque::new();
        let mut normal = VecDeque::new();
        assert_eq!(collector.drain_into(&mut reserved, &mut normal), 3);
        while let Some(record) = reserved.pop_front().or_else(|| normal.pop_front()) {
            collector.release(record.queue);
        }
        assert_eq!(shared.metrics.normal_queue_depth.load(Ordering::Relaxed), 0);

        collector.drain_into(&mut reserved, &mut normal);
        assert_eq!(collector.registered_lanes(), 1);
        assert_eq!(shared.metrics.producer_lanes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn enforces_global_normal_and_reserved_capacity() {
        let shared = shared();
        let collector = LogCollector::new(1, 1, Arc::clone(&shared));

        assert!(matches!(collector.submit(log("normal"), false), SubmitResult::Accepted));
        assert!(matches!(collector.submit(log("normal-full"), false), SubmitResult::Full));
        assert!(matches!(collector.submit(log("reserved"), true), SubmitResult::Accepted));
        assert!(matches!(collector.submit(log("reserved-full"), true), SubmitResult::Full));
        assert_eq!(shared.metrics.normal_queue_depth.load(Ordering::Relaxed), 1);
        assert_eq!(shared.metrics.reserved_queue_depth.load(Ordering::Relaxed), 1);

        let mut reserved = VecDeque::new();
        let mut normal = VecDeque::new();
        collector.drain_into(&mut reserved, &mut normal);
        assert_eq!(reserved.front().map(|record| record.queue), Some(QueueKind::Reserved));
        while let Some(record) = reserved.pop_front().or_else(|| normal.pop_front()) {
            collector.release(record.queue);
        }
        assert_eq!(shared.metrics.normal_queue_depth.load(Ordering::Relaxed), 0);
        assert_eq!(shared.metrics.reserved_queue_depth.load(Ordering::Relaxed), 0);
    }

    #[hegel::test(test_cases = 300)]
    fn generated_admission_sequences_preserve_capacity_order_and_identity(tc: TestCase) {
        let normal_capacity = tc.draw(gs::integers::<u8>().max_value(32)) as usize;
        let reserved_capacity = tc.draw(gs::integers::<u8>().max_value(16)) as usize;
        let priorities = tc.draw(gs::vecs(gs::booleans()).max_size(128));
        let shared = shared();
        let collector = LogCollector::new(normal_capacity, reserved_capacity, Arc::clone(&shared));
        let lane = collector.register_lane();
        let mut expected_normal = Vec::new();
        let mut expected_reserved = Vec::new();

        for (index, priority) in priorities.into_iter().enumerate() {
            let message = index.to_string();
            let expected_accepted = if expected_normal.len() < normal_capacity {
                expected_normal.push(message.clone());
                true
            } else if priority && expected_reserved.len() < reserved_capacity {
                expected_reserved.push(message.clone());
                true
            } else {
                false
            };
            let result = collector.submit_to_lane(&lane, log(&message), priority);
            assert_eq!(matches!(result, SubmitResult::Accepted), expected_accepted);
        }

        assert_eq!(shared.metrics.normal_queue_depth.load(Ordering::Relaxed) as usize, expected_normal.len());
        assert_eq!(shared.metrics.reserved_queue_depth.load(Ordering::Relaxed) as usize, expected_reserved.len());

        let mut reserved = VecDeque::new();
        let mut normal = VecDeque::new();
        while collector.drain_into(&mut reserved, &mut normal) > 0 {}

        assert_eq!(reserved.iter().map(|record| record.log.message.clone()).collect::<Vec<_>>(), expected_reserved);
        assert!(reserved.iter().all(|record| record.queue == QueueKind::Reserved));
        assert_eq!(normal.iter().map(|record| record.log.message.clone()).collect::<Vec<_>>(), expected_normal);
        assert!(normal.iter().all(|record| record.queue == QueueKind::Normal));

        while let Some(record) = reserved.pop_front().or_else(|| normal.pop_front()) {
            collector.release(record.queue);
        }
        assert_eq!(shared.metrics.normal_queue_depth.load(Ordering::Relaxed), 0);
        assert_eq!(shared.metrics.reserved_queue_depth.load(Ordering::Relaxed), 0);
    }
}
