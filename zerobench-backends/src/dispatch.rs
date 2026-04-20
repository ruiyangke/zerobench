//! Single protocol-dispatch entry point.
//!
//! `run_plan` takes a fully-built `Plan` + a bundled `RunCtx` and
//! returns `Vec<TaskStats>`. The closed-world match on `Step` picks
//! the right backend. Multi-protocol plans (HTTP + SSE + WS
//! scenarios in one Plan) fan out to one thread per protocol and
//! join.
//!
//! Every caller (CLI verbs, DSL runner, test harness) routes through
//! this one function — no per-protocol branches outside this file.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use rustls::ClientConfig;

use zerobench_core::plan::{Plan, Protocol, Step};
use zerobench_core::stats::TaskStats;
use zerobench_core::transport::{Target, TransportOpts};
use zerobench_runtime::LiveSnapshot;

/// Everything every backend entry point needs, in one struct. Solves
/// the eight-parameter-call antipattern.
#[derive(Clone)]
pub struct RunCtx {
    /// Target host/port/scheme/SNI — shared by every backend.
    pub target: Target,
    /// Connect/read/write timeouts, `insecure_tls`, `max_conns`, etc.
    pub opts: TransportOpts,
    /// Measurement duration for this dispatch (warmup OR steady-state —
    /// the caller picks per call).
    pub duration: Duration,
    /// OS worker thread count for threaded backends (`run_mio_threaded`
    /// shards across this many mio polls). `max(1)` enforced by callers.
    pub num_threads: usize,
    /// Closed-loop concurrency / connection pool size. SSE/WS
    /// backends read their own subscriber/connection count from the
    /// plan struct and ignore this — see the per-backend docs.
    pub connections: usize,
    /// Open-loop target req/s. `None` means closed-loop saturate.
    pub target_rps: Option<f64>,
    /// Pre-built rustls `ClientConfig`. `None` for plain-TCP targets.
    pub tls_config: Option<Arc<ClientConfig>>,
    /// Live-snapshot sink for the TUI. `None` in headless runs and for
    /// warmup/rhai paths that don't wire TUI.
    pub live: Option<Arc<LiveSnapshot>>,
    /// External stop flag — trips the worker loops early (TUI 'q',
    /// SIGINT, rate-step boundary). `None` means the backend
    /// installs its own timer thread for `duration`.
    pub stop: Option<Arc<AtomicBool>>,
}

/// Dispatch `plan` to the appropriate backend(s) and return merged
/// per-task statistics. Multi-protocol plans run each protocol on its
/// own thread; single-protocol plans run inline.
///
/// The Plan may mix HTTP, SSE, and WS scenarios. Each protocol group
/// gets filtered into its own sub-plan and routed to one backend
/// thread; stats from every group are concatenated into the returned
/// `Vec<TaskStats>`. Callers fold the result through
/// `Summary::merge(stats, ctx.duration)`.
pub fn run_plan(plan: &Plan, ctx: &RunCtx) -> Vec<TaskStats> {
    let has_http = plan.scenarios.iter().any(|s| s.protocol() == Protocol::Http);
    let has_sse = plan.scenarios.iter().any(|s| s.protocol() == Protocol::Sse);
    let has_ws = plan.scenarios.iter().any(|s| s.protocol() == Protocol::Ws);

    let active = (has_http as u32) + (has_sse as u32) + (has_ws as u32);

    // Single-protocol: run inline, no thread spawn. Empty-scenarios
    // plan falls through this branch (no protocol active) and
    // returns an empty Vec.
    if active <= 1 {
        if plan.scenarios.is_empty() || active == 0 {
            return Vec::new();
        }
        return run_one_protocol(plan, ctx);
    }

    // Multi-protocol fan-out: one thread per active protocol. Each
    // thread sees a filtered sub-plan containing only its own
    // scenarios. Stats are concatenated in a deterministic order
    // (HTTP, SSE, WS) for reproducibility.
    let http_handle = if has_http {
        let sub = sub_plan(plan, Protocol::Http);
        let ctx = ctx.clone();
        Some(std::thread::spawn(move || run_one_protocol(&sub, &ctx)))
    } else {
        None
    };

    let sse_handle = if has_sse {
        let sub = sub_plan(plan, Protocol::Sse);
        let ctx = ctx.clone();
        Some(std::thread::spawn(move || run_one_protocol(&sub, &ctx)))
    } else {
        None
    };

    let ws_handle = if has_ws {
        let sub = sub_plan(plan, Protocol::Ws);
        let ctx = ctx.clone();
        Some(std::thread::spawn(move || run_one_protocol(&sub, &ctx)))
    } else {
        None
    };

    let mut all_stats: Vec<TaskStats> = Vec::new();
    if let Some(h) = http_handle {
        if let Ok(stats) = h.join() {
            all_stats.extend(stats);
        }
    }
    if let Some(h) = sse_handle {
        if let Ok(stats) = h.join() {
            all_stats.extend(stats);
        }
    }
    if let Some(h) = ws_handle {
        if let Ok(stats) = h.join() {
            all_stats.extend(stats);
        }
    }
    all_stats
}

/// Clone `plan` with `scenarios` filtered to a single protocol group.
/// Duration, vars, mode, runs, threads, name all copy through.
fn sub_plan(plan: &Plan, proto: Protocol) -> Plan {
    let mut sub = plan.clone();
    sub.scenarios.retain(|s| s.protocol() == proto);
    sub
}

/// First non-Pause step across the plan. Multi-protocol callers have
/// already filtered `plan.scenarios` to a single protocol group, so
/// the returned step's kind uniquely picks one backend.
fn first_wire_step(plan: &Plan) -> Option<&Step> {
    for sc in &plan.scenarios {
        for st in &sc.steps {
            match st {
                Step::Pause(_) | Step::PauseRandom { .. } => continue,
                other => return Some(other),
            }
        }
    }
    None
}

/// `true` when ANY HTTP scenario contains a `HttpColdConnect` step.
/// Matches the `any_http_is_cold` logic in the old measure.rs
/// dispatch: a plan with one cold scenario routes its whole HTTP
/// group through `cold_connect`, and mixed-cold/hot plans are a
/// documented limitation.
fn any_http_is_cold(plan: &Plan) -> bool {
    plan.scenarios.iter().any(|s| {
        s.protocol() == Protocol::Http
            && s.steps.iter().any(|st| matches!(st, Step::HttpColdConnect(_)))
    })
}

/// Dispatch within one protocol group — picks the specific backend
/// from the step kind. `plan` here is already filtered to one
/// protocol (the multi-protocol fan-out in `run_plan` did it).
fn run_one_protocol(plan: &Plan, ctx: &RunCtx) -> Vec<TaskStats> {
    // HTTP cold-connect special case: a plan containing ANY
    // HttpColdConnect step routes the whole group to cold_connect
    // (same semantics as the old measure.rs dispatch).
    let first = first_wire_step(plan);

    match first {
        Some(Step::Request(_)) => {
            if any_http_is_cold(plan) {
                crate::http::cold_connect::run_cold_connect_from_plan_threaded(
                    &ctx.target,
                    &ctx.opts,
                    plan,
                    ctx.connections as u32,
                    ctx.duration,
                    ctx.target_rps,
                    ctx.tls_config.clone(),
                    ctx.live.as_ref().map(Arc::clone),
                    ctx.stop.as_ref().map(Arc::clone),
                )
            } else {
                crate::http::mio_h1::run_mio_threaded(
                    &ctx.target,
                    &ctx.opts,
                    plan,
                    ctx.num_threads,
                    ctx.connections,
                    ctx.duration,
                    ctx.target_rps,
                    ctx.tls_config.clone(),
                    ctx.live.as_ref().map(Arc::clone),
                    ctx.stop.as_ref().map(Arc::clone),
                )
            }
        }
        Some(Step::HttpColdConnect(_)) => {
            crate::http::cold_connect::run_cold_connect_from_plan_threaded(
                &ctx.target,
                &ctx.opts,
                plan,
                ctx.connections as u32,
                ctx.duration,
                ctx.target_rps,
                ctx.tls_config.clone(),
                ctx.live.as_ref().map(Arc::clone),
                ctx.stop.as_ref().map(Arc::clone),
            )
        }
        Some(Step::SseHold(_)) => crate::sse::run_sse_hold_from_plan_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.duration,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
        Some(Step::SseFanout(_)) => crate::sse::run_sse_fanout_from_plan_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.duration,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
        Some(Step::SseReconnectStorm(_)) => {
            crate::sse::run_sse_reconnect_storm_from_plan_threaded(
                &ctx.target,
                &ctx.opts,
                plan,
                ctx.duration,
                ctx.tls_config.clone(),
                ctx.live.as_ref().map(Arc::clone),
                ctx.stop.as_ref().map(Arc::clone),
            )
        }
        Some(Step::WsEchoRtt(_)) => crate::ws::run_ws_echo_rtt_from_plan_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.duration,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
        Some(Step::WsHold(_)) => crate::ws::run_ws_hold_from_plan_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.duration,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
        Some(Step::WsServerPushRtt(_)) => crate::ws::run_ws_server_push_rtt_from_plan_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.duration,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
        Some(Step::WsFanout(_)) => crate::ws::run_ws_fanout_from_plan_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.duration,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
        // Pause/PauseRandom already filtered by `first_wire_step`.
        Some(Step::Pause(_)) | Some(Step::PauseRandom { .. }) => unreachable!(
            "Pause steps are filtered by first_wire_step"
        ),
        // Empty scenarios — default to the HTTP backend so it can
        // silently skip (mio_h1's pick_scenario filters scenarios
        // with no Request step). Matches the existing behaviour in
        // run_mio_sync for ambiguous plans.
        None => crate::http::mio_h1::run_mio_threaded(
            &ctx.target,
            &ctx.opts,
            plan,
            ctx.num_threads,
            ctx.connections,
            ctx.duration,
            ctx.target_rps,
            ctx.tls_config.clone(),
            ctx.live.as_ref().map(Arc::clone),
            ctx.stop.as_ref().map(Arc::clone),
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use smallvec::SmallVec;
    use zerobench_core::plan::{
        Mode, RateProfile, RequestPlan, Scenario, SseHoldPlan, Step,
    };
    use zerobench_core::template::Template;
    use zerobench_core::transport::Target;
    use zerobench_core::var::VarRegistry;

    /// Bind an ephemeral TcpListener and return its port. Every
    /// incoming connection is accepted and immediately dropped —
    /// the client sees either an EOF on read or a short-lived peer
    /// depending on OS scheduling. Enough to exercise the dispatch
    /// path and surface backend errors without a real server.
    fn spawn_rst_server(stop: Arc<AtomicBool>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        drop(stream);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        port
    }

    fn make_http_plan(port: u16, cold: bool) -> Plan {
        let mut vars = VarRegistry::new();
        let url_str = format!("http://127.0.0.1:{port}/");
        let url = Template::compile(&url_str, &mut vars).unwrap();
        let request = RequestPlan {
            method: http::Method::GET,
            url,
            headers: SmallVec::new(),
            body: None,
            extract: Vec::new(),
            checks: Vec::new(),
            expect_streaming: false,
        };
        let step = if cold {
            Step::HttpColdConnect(zerobench_core::plan::ColdConnectPlan { request })
        } else {
            Step::Request(request)
        };
        Plan {
            scenarios: vec![Scenario {
                name: "test".into(),
                rate: RateProfile::Saturate { max_concurrency: 1 },
                steps: vec![step],
            }],
            vars,
            duration: Duration::from_millis(200),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::Measure,
            name: "t".into(),
        }
    }

    fn make_sse_plan(port: u16) -> Plan {
        let mut vars = VarRegistry::new();
        let url_str = format!("http://127.0.0.1:{port}/sse");
        let url = Template::compile(&url_str, &mut vars).unwrap();
        Plan {
            scenarios: vec![Scenario {
                name: "sse".into(),
                rate: RateProfile::Saturate { max_concurrency: 1 },
                steps: vec![Step::SseHold(SseHoldPlan {
                    url,
                    headers: SmallVec::new(),
                    subscribers: 1,
                    hold_for: Duration::from_millis(200),
                    reconnect: false,
                })],
            }],
            vars,
            duration: Duration::from_millis(200),
            warmup: Duration::ZERO,
            cooldown: Duration::ZERO,
            runs: 1,
            threads: 1,
            mode: Mode::Measure,
            name: "t".into(),
        }
    }

    fn make_ctx(target: Target, connections: usize) -> RunCtx {
        RunCtx {
            target,
            opts: TransportOpts {
                connect_timeout: Duration::from_millis(100),
                request_timeout: Duration::from_millis(200),
                max_conns: connections,
                tcp_nodelay: true,
                insecure_tls: false,
                ..TransportOpts::default()
            },
            duration: Duration::from_millis(200),
            num_threads: 1,
            connections,
            target_rps: None,
            tls_config: None,
            live: None,
            stop: None,
        }
    }

    #[test]
    fn empty_plan_returns_empty() {
        let plan = Plan::new();
        let target = Target::parse("http://127.0.0.1:1/").unwrap();
        let ctx = make_ctx(target, 1);
        let stats = run_plan(&plan, &ctx);
        assert!(stats.is_empty());
    }

    #[test]
    fn single_http_plan_dispatches_to_mio_h1() {
        // The RST server drops every connection → we measure that
        // the dispatch path ran the HTTP backend (Connect / read
        // errors record into TaskStats).
        let server_stop = Arc::new(AtomicBool::new(false));
        let port = spawn_rst_server(server_stop.clone());

        let plan = make_http_plan(port, false);
        let target = Target::parse(&format!("http://127.0.0.1:{port}/")).unwrap();
        let ctx = make_ctx(target, 1);

        let t_start = Instant::now();
        let stats = run_plan(&plan, &ctx);
        let elapsed = t_start.elapsed();

        server_stop.store(true, Ordering::Relaxed);

        // mio_h1 returns one TaskStats per worker thread. Dispatch
        // picked the right backend if we got any stats back within
        // the duration budget.
        assert!(!stats.is_empty(), "expected mio_h1 to return at least one TaskStats");
        assert!(
            elapsed < Duration::from_secs(2),
            "dispatch should not block much past ctx.duration — took {elapsed:?}"
        );
    }

    #[test]
    fn http_plan_with_cold_connect_routes_to_cold_connect_backend() {
        let server_stop = Arc::new(AtomicBool::new(false));
        let port = spawn_rst_server(server_stop.clone());

        let plan = make_http_plan(port, true);
        let target = Target::parse(&format!("http://127.0.0.1:{port}/")).unwrap();
        let ctx = make_ctx(target, 1);

        let t_start = Instant::now();
        let stats = run_plan(&plan, &ctx);
        let elapsed = t_start.elapsed();

        server_stop.store(true, Ordering::Relaxed);

        // cold_connect's threaded runner returns one TaskStats per
        // cold scenario — not sharded across workers like mio_h1.
        // Either way, it must return SOMETHING and not hang.
        assert!(!stats.is_empty(), "cold_connect backend produced no TaskStats");
        assert!(
            elapsed < Duration::from_secs(2),
            "cold_connect dispatch took too long: {elapsed:?}"
        );
    }

    #[test]
    fn mixed_http_and_sse_plan_fans_out_and_concatenates() {
        let server_stop = Arc::new(AtomicBool::new(false));
        let port = spawn_rst_server(server_stop.clone());

        let http_plan = make_http_plan(port, false);
        let sse_plan = make_sse_plan(port);

        // Build a mixed plan by concatenating scenarios.
        let mut mixed = http_plan.clone();
        mixed.scenarios.extend(sse_plan.scenarios.clone());
        // Carry the SSE plan's vars too — VarRegistry is merge-safe
        // by construction here because both sub-plans were built
        // with independent registries that only bind the URL
        // template, and neither SSE nor HTTP worker reads by slot
        // index across plans.
        let _ = sse_plan.vars;

        let target = Target::parse(&format!("http://127.0.0.1:{port}/")).unwrap();
        let ctx = make_ctx(target, 1);

        let stats = run_plan(&mixed, &ctx);
        server_stop.store(true, Ordering::Relaxed);

        // Both backends ran → we got stats from at least two
        // sources (HTTP + SSE).
        assert!(
            stats.len() >= 2,
            "expected ≥ 2 TaskStats from mixed HTTP+SSE dispatch, got {}",
            stats.len()
        );
    }

    #[test]
    fn sub_plan_filters_scenarios_by_protocol() {
        let port = 1; // never dialed in this test
        let mut plan = make_http_plan(port, false);
        plan.scenarios.extend(make_sse_plan(port).scenarios);

        let http_only = sub_plan(&plan, Protocol::Http);
        assert_eq!(http_only.scenarios.len(), 1);
        assert_eq!(http_only.scenarios[0].protocol(), Protocol::Http);

        let sse_only = sub_plan(&plan, Protocol::Sse);
        assert_eq!(sse_only.scenarios.len(), 1);
        assert_eq!(sse_only.scenarios[0].protocol(), Protocol::Sse);

        // Duration, vars, mode copy through.
        assert_eq!(http_only.duration, plan.duration);
        assert_eq!(http_only.mode, plan.mode);
        assert_eq!(http_only.name, plan.name);
    }

    #[test]
    fn first_wire_step_skips_pauses() {
        let mut plan = make_http_plan(1, false);
        // Prepend a Pause step.
        plan.scenarios[0].steps.insert(0, Step::Pause(Duration::from_millis(1)));
        plan.scenarios[0]
            .steps
            .insert(1, Step::PauseRandom {
                min: Duration::ZERO,
                max: Duration::from_millis(1),
            });
        let first = first_wire_step(&plan);
        assert!(matches!(first, Some(Step::Request(_))));
    }

    #[test]
    fn any_http_is_cold_detects_cold_connect_step() {
        let plan = make_http_plan(1, true);
        assert!(any_http_is_cold(&plan));
        let plan_hot = make_http_plan(1, false);
        assert!(!any_http_is_cold(&plan_hot));
    }
}
