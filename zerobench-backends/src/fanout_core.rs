//! Shared fanout helpers.
//!
//! SSE fanout (`sse/fanout.rs`) and WS fanout (`ws/fanout.rs`) run the
//! same trigger skeleton: fire an HTTP POST every `TRIGGER_INTERVAL_MS`
//! ms, record each send instant, correlate later against the frames
//! / events the held subscribers observed. The subscriber side is
//! protocol-specific; the trigger side is identical — this module is
//! the shared trigger side.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustls::ClientConfig;

use zerobench_core::transport::{Target, TransportOpts};
use zerobench_core::{ExpandCtx, Template};

use crate::http::simple_post::fire_http_post;

/// Cadence of the trigger loop (ms). Both SSE and WS fanout today use
/// 500 ms — a compromise between overhead and broadcast resolution.
pub const TRIGGER_INTERVAL_MS: u64 = 500;

/// Fire HTTP POST triggers at `TRIGGER_INTERVAL_MS`, recording each
/// send instant into `triggers`. The trigger URL is re-expanded from
/// its template every firing so `{{counter}}` / `{{uuid}}` /
/// `{{now_ns}}` advance per trigger.
pub fn run_trigger_loop(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &Template,
    deadline: Instant,
    stop: &AtomicBool,
    triggers: &Mutex<Vec<Instant>>,
    tls_config: Option<&Arc<ClientConfig>>,
) {
    let interval = Duration::from_millis(TRIGGER_INTERVAL_MS);
    let mut next = Instant::now() + interval;
    let mut rng = zerobench_core::rng::from_entropy();
    let counter = std::rc::Rc::new(std::cell::Cell::new(0u64));
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let now = Instant::now();
        if now < next {
            std::thread::sleep((next - now).min(Duration::from_millis(100)));
            continue;
        }
        let mut url_buf: Vec<u8> = Vec::with_capacity(128);
        {
            let mut ctx = ExpandCtx {
                rng: &mut rng,
                counter: &counter,
                scenario_vars: &[],
            };
            trigger_url.expand_into(&mut url_buf, &mut ctx);
        }
        let url_str = String::from_utf8_lossy(&url_buf).to_string();
        let t = Instant::now();
        if fire_trigger(target, opts, &url_str, tls_config).is_ok() {
            triggers.lock().expect("triggers mutex").push(t);
        }
        next = Instant::now() + interval;
    }
}

/// Send one non-blocking HTTP POST to `trigger_url`.
///
/// The trigger URL typically points at the same host — cross-host
/// triggers aren't supported (requires a second `Target`). Only the
/// path+query portion of the URL is sent; scheme/authority use `target`.
pub fn fire_trigger(
    target: &Target,
    opts: &TransportOpts,
    trigger_url: &str,
    tls_config: Option<&Arc<ClientConfig>>,
) -> std::io::Result<()> {
    let path = match trigger_url.find("://").and_then(|i| trigger_url[i + 3..].find('/')) {
        Some(rel) => {
            let abs_idx = trigger_url.find("://").map(|i| i + 3).unwrap_or(0) + rel;
            &trigger_url[abs_idx..]
        }
        None => "/",
    };
    fire_http_post(target, opts, path, &[], tls_config)
}

/// Render a static template to a String. Fanout triggers can't use
/// per-iteration variables today (no scenario context at trigger time).
pub fn render_template(tpl: &Template) -> String {
    let mut buf = Vec::with_capacity(256);
    let mut rng = zerobench_core::rng::from_entropy();
    let mut ctx = ExpandCtx {
        rng: &mut rng,
        counter: &std::rc::Rc::new(std::cell::Cell::new(0)),
        scenario_vars: &[],
    };
    tpl.expand_into(&mut buf, &mut ctx);
    String::from_utf8_lossy(&buf).to_string()
}

/// Extract the path-and-query portion of an absolute URL template.
/// Returns `"/"` if none is present.
///
/// Used by SSE + WS fanout to split a subscribe URL into its path
/// before writing it into a wire request.
pub fn extract_path(url: &Template) -> String {
    let s = render_template(url);
    if let Some(path_start) = s.find("://").and_then(|i| s[i + 3..].find('/').map(|j| i + 3 + j)) {
        s[path_start..].to_string()
    } else {
        "/".to_string()
    }
}
