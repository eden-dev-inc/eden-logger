use eden_logger::{LogAudience, ctx_with_trace, log_error, log_info, log_warn};
use function_name::named;

#[named]
fn main() {
    let _ctx = ctx_with_trace!();

    log_info!(_ctx.clone(), "This is an info message", audience = LogAudience::Internal);
    log_warn!(_ctx.clone(), "This is a warning message", audience = LogAudience::Internal);
    log_error!(_ctx, "This is an error message", audience = LogAudience::Internal);
}
