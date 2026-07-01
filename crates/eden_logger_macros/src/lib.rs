#![cfg_attr(test, allow(clippy::unwrap_used))]
// This proc_macro crate uses extensive conditional compilation based on log-level features.
// Variables are parsed for validation but may be unused when features are disabled.
#![allow(unused_variables)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Expr, Ident, Result, Token,
    parse::{Parse, ParseStream},
};

// These structs are used conditionally when log-level features are enabled.
// Clippy sees them as dead code when compiling without those features.
#[allow(dead_code)]
/// audience = <expr>
#[derive(Clone)]
struct AudienceOpt(Option<Expr>);

#[allow(dead_code)]
/// key = <expr>
#[derive(Clone)]
struct Kv {
    key: Ident,
    _eq: Token![=],
    val: Expr,
}

#[allow(dead_code)]
/// (ctx_expr, msg_expr [, audience = expr] [, k = v]*)
#[derive(Clone)]
struct LogArgs {
    ctx: Expr,
    _comma1: Token![,],
    msg: Expr,
    rest: Vec<RestItem>,
}

#[allow(dead_code)]
#[derive(Clone)]
enum RestItem {
    Audience {
        _comma: Token![,],
        _audience_kw: Ident, // must be "audience"
        _eq: Token![=],
        expr: Expr,
    },
    Kv {
        _comma: Token![,],
        kv: Kv,
    },
}

impl Parse for Kv {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            key: input.parse()?,
            _eq: input.parse()?,
            val: input.parse()?,
        })
    }
}

impl Parse for LogArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let ctx: Expr = input.parse()?;
        let _comma1: Token![,] = input.parse()?;
        let msg: Expr = input.parse()?;

        let mut rest = Vec::new();
        while input.peek(Token![,]) {
            let _comma: Token![,] = input.parse()?;

            // Peek for `audience =`
            if input.peek(Ident) && input.peek2(Token![=]) {
                let ident: Ident = input.parse()?;
                if ident == "audience" {
                    let _eq: Token![=] = input.parse()?;
                    let expr: Expr = input.parse()?;
                    rest.push(RestItem::Audience { _comma, _audience_kw: ident, _eq, expr });
                    continue;
                } else {
                    // treat as key=value
                    let _eq: Token![=] = input.parse()?;
                    let val: Expr = input.parse()?;
                    rest.push(RestItem::Kv { _comma, kv: Kv { key: ident, _eq, val } });
                    continue;
                }
            }

            // Fallback: key=value
            let kv: Kv = input.parse()?;
            rest.push(RestItem::Kv { _comma, kv });
        }

        Ok(Self { ctx, _comma1, msg, rest })
    }
}

#[allow(dead_code)] // Used conditionally when log-level features are enabled
fn split_args(args: &LogArgs) -> (Expr, Expr, AudienceOpt, Vec<Kv>) {
    let ctx = args.ctx.clone();
    let msg = args.msg.clone();

    let mut audience: Option<Expr> = None;
    let mut kvs = Vec::new();

    for item in &args.rest {
        match item {
            RestItem::Audience { expr, .. } => audience = Some(expr.clone()),
            RestItem::Kv { kv, .. } => kvs.push(kv.clone()),
        }
    }

    (ctx, msg, AudienceOpt(audience), kvs)
}

// Compile-time booleans baked from THIS crate's features.
// These are used conditionally in gen_body when log-level features are enabled.
#[allow(dead_code)]
#[cfg(feature = "log-client")]
const CLIENT_ENABLED: bool = true;
#[allow(dead_code)]
#[cfg(not(feature = "log-client"))]
const CLIENT_ENABLED: bool = false;

#[allow(dead_code)]
#[cfg(feature = "log-internal")]
const INTERNAL_ENABLED: bool = true;
#[allow(dead_code)]
#[cfg(not(feature = "log-internal"))]
const INTERNAL_ENABLED: bool = false;

#[allow(dead_code)]
#[cfg(feature = "log-both")]
const BOTH_ENABLED: bool = true;
#[allow(dead_code)]
#[cfg(not(feature = "log-both"))]
const BOTH_ENABLED: bool = false;

#[allow(dead_code)]
#[cfg(feature = "source-location")]
const SOURCE_LOCATION_ENABLED: bool = true;
#[allow(dead_code)]
#[cfg(not(feature = "source-location"))]
const SOURCE_LOCATION_ENABLED: bool = false;

// ---------- codegen helpers ----------
#[allow(dead_code)] // Used conditionally when log-level features are enabled
fn gen_body(level_ident: &str, ctx: Expr, msg: Expr, aud_opt: AudienceOpt, kvs: &[Kv]) -> proc_macro2::TokenStream {
    let aud_expr = if let Some(a) = aud_opt.0 {
        a
    } else {
        syn::parse_quote!(::eden_logger::LogAudience::Internal)
    };

    // Bake feature availability as literal booleans into the expanded code:
    let client_enabled = CLIENT_ENABLED;
    let internal_enabled = INTERNAL_ENABLED;
    let both_enabled = BOTH_ENABLED;

    // Build k-v pair bindings and array entries for emit_direct
    let kv_bindings: Vec<_> = kvs
        .iter()
        .enumerate()
        .map(|(i, Kv { val, .. })| {
            let var = syn::Ident::new(&format!("__kv{i}"), proc_macro2::Span::call_site());
            quote! { let #var = &(#val) as &dyn ::core::fmt::Display; }
        })
        .collect();

    let kv_entries: Vec<_> = kvs
        .iter()
        .enumerate()
        .map(|(i, Kv { key, .. })| {
            let var = syn::Ident::new(&format!("__kv{i}"), proc_macro2::Span::call_site());
            quote! { (stringify!(#key), #var) }
        })
        .collect();

    let (file_expr, line_expr) = if SOURCE_LOCATION_ENABLED {
        (quote! { ::core::option::Option::Some(file!()) }, quote! { ::core::option::Option::Some(line!()) })
    } else {
        (quote! { ::core::option::Option::None }, quote! { ::core::option::Option::None })
    };

    let level = syn::Ident::new(level_ident, proc_macro2::Span::call_site());

    quote!({
        let __aud = #aud_expr;
        let __enabled: bool = match __aud {
            ::eden_logger::LogAudience::Client   => #client_enabled,
            ::eden_logger::LogAudience::Internal => #internal_enabled,
            ::eden_logger::LogAudience::Both     => #both_enabled,
        };

        if __enabled {
            let __msg = #msg;
            #(#kv_bindings)*
            ::eden_logger::emit_direct(
                ::eden_logger::LogLevel::#level,
                &__msg,
                &#ctx,
                __aud,
                &[#(#kv_entries),*],
                #file_expr,
                #line_expr,
            );
        }
    })
}

/// Log a trace-level message.
///
/// # Syntax
/// ```ignore
/// log_trace!(ctx, "message", audience = LogAudience::Internal)
/// log_trace!(ctx, "message", audience = LogAudience::Internal, key1 = value1)
/// ```
///
/// # Parameters
/// - `ctx: LogContext` - The logging context
/// - `message: &str` - The log message
/// - `audience: LogAudience` - Who should see this (Internal/Client/Both)
/// - Additional key-value pairs for metadata (optional)
#[proc_macro]
pub fn log_trace(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as LogArgs);

    // If the *level* is disabled in THIS crate, emit a no-op immediately.
    #[cfg(not(feature = "log-trace"))]
    {
        quote!({}).into()
    }

    #[cfg(feature = "log-trace")]
    {
        let (ctx, msg, aud, kvs) = split_args(&args);
        gen_body("Trace", ctx, msg, aud, &kvs).into()
    }
}

/// Log a debug-level message.
///
/// # Syntax
/// ```ignore
/// log_debug!(ctx, "message", audience = LogAudience::Internal)
/// log_debug!(ctx, "message", audience = LogAudience::Internal, key1 = value1)
/// ```
///
/// # Parameters
/// - `ctx: LogContext` - The logging context
/// - `message: &str` - The log message
/// - `audience: LogAudience` - Who should see this (Internal/Client/Both)
/// - Additional key-value pairs for metadata (optional)
#[proc_macro]
pub fn log_debug(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as LogArgs);

    #[cfg(not(feature = "log-debug"))]
    {
        quote!({}).into()
    }

    #[cfg(feature = "log-debug")]
    {
        let (ctx, msg, aud, kvs) = split_args(&args);
        gen_body("Debug", ctx, msg, aud, &kvs).into()
    }
}

/// Log an info-level message.
///
/// # Syntax
/// ```ignore
/// log_info!(ctx, "message", audience = LogAudience::Internal)
/// log_info!(ctx, "message", audience = LogAudience::Internal, key1 = value1)
/// ```
///
/// # Parameters
/// - `ctx: LogContext` - The logging context
/// - `message: &str` - The log message
/// - `audience: LogAudience` - Who should see this (Internal/Client/Both)
/// - Additional key-value pairs for metadata (optional)
#[proc_macro]
pub fn log_info(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as LogArgs);

    #[cfg(not(feature = "log-info"))]
    {
        quote!({}).into()
    }

    #[cfg(feature = "log-info")]
    {
        let (ctx, msg, aud, kvs) = split_args(&args);
        gen_body("Info", ctx, msg, aud, &kvs).into()
    }
}

/// Log a warning-level message.
///
/// # Syntax
/// ```ignore
/// log_warn!(ctx, "message", audience = LogAudience::Internal)
/// log_warn!(ctx, "message", audience = LogAudience::Internal, key1 = value1)
/// ```
///
/// # Parameters
/// - `ctx: LogContext` - The logging context
/// - `message: &str` - The log message
/// - `audience: LogAudience` - Who should see this (Internal/Client/Both)
/// - Additional key-value pairs for metadata (optional)
#[proc_macro]
pub fn log_warn(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as LogArgs);

    #[cfg(not(feature = "log-warn"))]
    {
        quote!({}).into()
    }

    #[cfg(feature = "log-warn")]
    {
        let (ctx, msg, aud, kvs) = split_args(&args);
        gen_body("Warn", ctx, msg, aud, &kvs).into()
    }
}

/// Log an error-level message.
///
/// # Syntax
/// ```ignore
/// log_error!(ctx, "message", audience = LogAudience::Internal)
/// log_error!(ctx, "message", audience = LogAudience::Internal, key1 = value1)
/// ```
///
/// # Parameters
/// - `ctx: LogContext` - The logging context
/// - `message: &str` - The log message
/// - `audience: LogAudience` - who should see this (Internal/Client/Both)
/// - Additional key-value pairs for metadata (optional)
#[proc_macro]
pub fn log_error(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as LogArgs);

    #[cfg(not(feature = "log-error"))]
    {
        quote!({}).into()
    }

    #[cfg(feature = "log-error")]
    {
        let (ctx, msg, aud, kvs) = split_args(&args);
        gen_body("Error", ctx, msg, aud, &kvs).into()
    }
}

// Convenience: make `ctx_with_trace!()` a proc-macro that calls
//
// Accepts an optional `RequestFields` type as the sole argument, e.g.
// `ctx_with_trace!(AppRequestFields)`. When omitted, defaults to `()`.
//
// When the `function-name` feature is enabled, the expansion also captures
// the enclosing function's name via `function_name!()` — the call site must
// then annotate that function with `#[function_name::named]`. When the
// feature is off, the expansion does not reference `function_name!` and
// `#[named]` is not required.
#[proc_macro]
pub fn ctx_with_trace(input: TokenStream) -> TokenStream {
    let input2: proc_macro2::TokenStream = input.into();
    let ty: proc_macro2::TokenStream = if input2.is_empty() {
        quote! { () }
    } else {
        input2
    };

    #[cfg(feature = "function-name")]
    {
        quote! {
            ::eden_logger::trace_context::<#ty>()
                .with_function(function_name!())
        }
        .into()
    }

    #[cfg(not(feature = "function-name"))]
    {
        quote! {
            ::eden_logger::trace_context::<#ty>()
        }
        .into()
    }
}
