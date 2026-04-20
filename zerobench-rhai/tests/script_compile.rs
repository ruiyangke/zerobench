//! Integration tests: load each example script and a handful of edge
//! cases, then assert on the finalized plan shape.
//!
//! The .rhai files under `examples/` at the workspace root are the
//! canonical smoke tests — if these fail, the DSL's contract has
//! shifted. The edge-case tests cover the error variants that need to
//! propagate cleanly to the CLI.

use http::Method;
use std::path::PathBuf;

use zerobench_core::plan::{RateProfile, Step};
use zerobench_rhai::{load_script, load_script_str, ScriptError};

/// Resolve `examples/NAME` against the workspace root. `CARGO_MANIFEST_DIR`
/// points at this crate, so we walk up one level.
fn example(name: &str) -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().join("examples").join(name)
}

// ---------------------------------------------------------------------------
// Example files
// ---------------------------------------------------------------------------

#[test]
fn simple_example_compiles_to_expected_plan() {
    let loaded = load_script(&example("simple.rhai")).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 1);
    assert_eq!(loaded.plan.scenarios[0].name, "events");
    assert_eq!(loaded.plan.scenarios[0].steps.len(), 1);

    match &loaded.plan.scenarios[0].steps[0] {
        Step::Request(r) => assert_eq!(r.method, Method::POST),
        other => panic!("expected Request, got {other:?}"),
    }

    match &loaded.plan.scenarios[0].rate {
        RateProfile::Constant(r) => {
            // 1k/s with a single scenario that didn't declare its own
            // weight falls back to auto_weight = 1/1 = 1.0 → full rate.
            assert!((*r - 1_000.0).abs() < 1e-6);
        }
        other => panic!("expected Constant, got {other:?}"),
    }
    assert_eq!(loaded.plan.duration, std::time::Duration::from_secs(10));
    // Target derived from the first URL's host+port.
    assert_eq!(loaded.target.host, "localhost");
    assert_eq!(loaded.target.port, 3000);
}

#[test]
fn chained_example_compiles_to_expected_plan() {
    let loaded = load_script(&example("chained.rhai")).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 1);
    let scn = &loaded.plan.scenarios[0];
    assert_eq!(scn.name, "user-session");
    // 3 steps since the example dropped its pause() step (the
    // corresponding Rhai function was removed as part of the
    // "no silent no-ops" DSL cleanup).
    assert_eq!(scn.steps.len(), 3);

    // Step 0: POST /login
    match &scn.steps[0] {
        Step::Request(r) => {
            assert_eq!(r.method, Method::POST);
            // Has exactly one extract (X-Auth-Token).
            assert_eq!(r.extract.len(), 1);
        }
        other => panic!("step 0: expected Request, got {other:?}"),
    }
    // Step 1: GET /me (with bearer template)
    match &scn.steps[1] {
        Step::Request(r) => {
            assert_eq!(r.method, Method::GET);
            assert_eq!(r.headers.len(), 1);
        }
        other => panic!("step 1: expected Request, got {other:?}"),
    }
    // Step 2: GET /api/feed
    match &scn.steps[2] {
        Step::Request(r) => assert_eq!(r.method, Method::GET),
        other => panic!("step 2: expected Request, got {other:?}"),
    }

    // `saturate(20)` + no global rate → Saturate(20) per scenario.
    match &scn.rate {
        RateProfile::Saturate { max_concurrency } => {
            assert_eq!(*max_concurrency, 20);
        }
        other => panic!("expected Saturate, got {other:?}"),
    }
    // Exactly one var ("token").
    assert_eq!(loaded.plan.vars.len(), 1);
    // Target: https://api.example.com:443
    assert_eq!(loaded.target.host, "api.example.com");
    assert_eq!(loaded.target.port, 443);
    assert!(loaded.target.tls);
}

// ---------------------------------------------------------------------------
// Error-path coverage
// ---------------------------------------------------------------------------

#[test]
fn missing_duration_surfaces_as_specific_error() {
    let src = r#"
        scenario("x", |s| { s.step(GET("http://h:1/")); });
        rate("1k/s");
    "#;
    assert!(matches!(
        load_script_str(src).unwrap_err(),
        ScriptError::MissingDuration
    ));
}

#[test]
fn no_scenarios_surfaces_as_specific_error() {
    let src = r#"
        duration("5s");
    "#;
    assert!(matches!(
        load_script_str(src).unwrap_err(),
        ScriptError::NoScenarios
    ));
}

#[test]
fn missing_env_without_default_surfaces_as_specific_error() {
    // The env var name is random enough that no real environment will
    // provide it — we just need to ensure the script references it via
    // `env("NAME")` and no default.
    let src = r#"
        let t = env("ZEROBENCH_TEST_DEFINITELY_UNSET_XYZ123");
        scenario("x", |s| {
            s.step(GET("http://h:1/"));
        });
        duration("5s");
    "#;
    let err = load_script_str(src).unwrap_err();
    match err {
        ScriptError::MissingEnv(name) => {
            assert_eq!(name, "ZEROBENCH_TEST_DEFINITELY_UNSET_XYZ123");
        }
        other => panic!("expected MissingEnv, got {other:?}"),
    }
}

#[test]
fn env_with_default_returns_default_when_unset() {
    let src = r#"
        scenario("x", |s| {
            s.step(
                GET("http://h:1/")
                    .header("X-Env", env("ZEROBENCH_TEST_UNSET_ABC", "fallback"))
            );
        });
        duration("5s");
    "#;
    // This must NOT fail — the default kicks in.
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 1);
}

#[test]
fn malformed_rhai_syntax_surfaces_parse_error() {
    let src = r#"
        this is not { valid rhai syntax !!!!
    "#;
    let err = load_script_str(src).unwrap_err();
    assert!(
        matches!(err, ScriptError::Parse(_) | ScriptError::Eval(_)),
        "expected Parse or Eval error, got {err:?}"
    );
}

#[test]
fn invalid_template_surfaces_template_error() {
    // `{{ not_a_known_variable }}` — the template engine will reject it
    // at finalize time.
    let src = r#"
        scenario("x", |s| {
            s.step(GET("http://h:1/{{not_a_known_variable}}"));
        });
        duration("5s");
    "#;
    let err = load_script_str(src).unwrap_err();
    match err {
        ScriptError::Template { scenario, field, .. } => {
            assert_eq!(scenario, "x");
            assert!(field.contains("url"), "field = {field}");
        }
        other => panic!("expected Template, got {other:?}"),
    }
}

#[test]
fn weighted_scenarios_distribute_global_rate_by_weight() {
    let src = r#"
        scenario("a", 0.3, |s| { s.step(GET("http://h:1/a")); });
        scenario("b", 0.7, |s| { s.step(GET("http://h:1/b")); });
        rate("10k/s");
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 2);

    // Total explicit weight is 1.0 so shares are 0.3 and 0.7 directly.
    match &loaded.plan.scenarios[0].rate {
        RateProfile::Constant(r) => assert!((*r - 3_000.0).abs() < 1e-6),
        other => panic!("expected Constant, got {other:?}"),
    }
    match &loaded.plan.scenarios[1].rate {
        RateProfile::Constant(r) => assert!((*r - 7_000.0).abs() < 1e-6),
        other => panic!("expected Constant, got {other:?}"),
    }
}

#[test]
fn global_rate_and_per_scenario_rate_is_conflict() {
    // Need a real conflict: both `rate(...)` at the top and `s.rate(...)`
    // inside the body.
    let src = r#"
        scenario("a", |s| {
            s.step(GET("http://h:1/"));
            s.rate("1k/s");
        });
        rate("10k/s");
        duration("5s");
    "#;
    let err = load_script_str(src).unwrap_err();
    assert!(
        matches!(err, ScriptError::ConflictingRate),
        "expected ConflictingRate, got {err:?}"
    );
}

#[test]
fn transport_helper_overrides_http_version() {
    let src = r#"
        transport("h2");
        scenario("x", |s| { s.step(GET("http://h:1/")); });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(
        loaded.http_version,
        zerobench_core::transport::HttpVersionPref::Http2
    );
}

#[test]
fn var_slot_reused_by_template_and_extractor() {
    // The script allocates `slot("token")`, uses it in an
    // `extract_header`, and references it via `{{var:token}}` — both
    // references should resolve to the same VarSlot.
    //
    // The helper is spelled `slot` (not `var`) because `var` is a
    // reserved keyword in Rhai.
    let src = r#"
        let tok = slot("token");
        scenario("x", |s| {
            s.step(
                POST("http://h:1/login")
                    .extract_header("X-Auth", tok)
            );
            s.step(
                GET("http://h:1/me")
                    .header("Authorization", "Bearer {{var:token}}")
            );
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    // Exactly one named slot: "token".
    assert_eq!(loaded.plan.vars.len(), 1);
    assert_eq!(loaded.plan.vars.name(zerobench_core::VarSlot(0)), Some("token"));
}

#[test]
fn pause_random_is_unregistered_and_fails_script_eval() {
    // pause / pause_random are deliberately NOT registered on the
    // Rhai engine today — no backend executes them, so exposing
    // them produced silent no-ops that misled users. Scripts using
    // them now fail at script-eval time with "unknown function",
    // which is the loudest possible signal that the feature is
    // not supported. `Step::PauseRandom` remains in the core Plan
    // enum so a future implementation can land without a schema
    // bump.
    let src = r#"
        scenario("x", |s| {
            s.step(pause_random("10ms", "20ms"));
            s.step(GET("http://h:1/"));
        });
        duration("5s");
    "#;
    assert!(
        load_script_str(src).is_err(),
        "pause_random() must fail at script-eval time"
    );
}

#[test]
fn body_literal_without_template_becomes_static_bytes() {
    let src = r#"
        scenario("x", |s| {
            s.step(
                POST("http://h:1/")
                    .body("plain raw body")
                    .expect_status(200)
            );
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    match &loaded.plan.scenarios[0].steps[0] {
        Step::Request(r) => match &r.body {
            Some(zerobench_core::BodySource::Static(b)) => {
                assert_eq!(b.as_ref(), b"plain raw body");
            }
            other => panic!("expected Static body, got {other:?}"),
        },
        other => panic!("expected Request, got {other:?}"),
    }
}

#[test]
fn json_body_adds_content_type_and_becomes_template() {
    let src = r#"
        scenario("x", |s| {
            s.step(
                POST("http://h:1/")
                    .json(#{ id: "{{uuid}}" })
                    .expect_status(201)
            );
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    match &loaded.plan.scenarios[0].steps[0] {
        Step::Request(r) => {
            assert!(matches!(&r.body, Some(zerobench_core::BodySource::Template(_))));
            // Exactly one header: Content-Type application/json.
            assert_eq!(r.headers.len(), 1);
        }
        other => panic!("expected Request, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// v0.1.0 protocol-native builders
// ---------------------------------------------------------------------------

#[test]
fn sse_hold_builder_compiles_to_step() {
    let src = r#"
        scenario("sse-events", |s| {
            s.step(sse_hold("http://api/stream", 100, "30s"));
        });
        duration("30s");
    "#;
    let loaded = load_script_str(src).expect("load");
    assert_eq!(loaded.plan.scenarios.len(), 1);
    match &loaded.plan.scenarios[0].steps[0] {
        Step::SseHold(p) => {
            assert_eq!(p.subscribers, 100);
            assert_eq!(p.hold_for, std::time::Duration::from_secs(30));
        }
        other => panic!("expected SseHold, got {other:?}"),
    }
}

#[test]
fn sse_hold_builder_accepts_integer_seconds() {
    let src = r#"
        scenario("sse", |s| {
            s.step(sse_hold("http://api/stream", 10, 5));
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::SseHold(p) => {
            assert_eq!(p.subscribers, 10);
            assert_eq!(p.hold_for, std::time::Duration::from_secs(5));
        }
        _ => panic!("expected SseHold"),
    }
}

#[test]
fn sse_hold_reconnect_setter() {
    let src = r#"
        scenario("sse", |s| {
            s.step(sse_hold("http://api/stream", 1, "1s").reconnect(false));
        });
        duration("1s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::SseHold(p) => {
            assert!(!p.reconnect, "reconnect(false) should disable reconnect");
        }
        _ => panic!("expected SseHold"),
    }
}

#[test]
fn ws_echo_rtt_builder_compiles_to_step() {
    let src = r#"
        scenario("ws", |s| {
            s.step(ws_echo_rtt("ws://api/chat", 50, 100));
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::WsEchoRtt(p) => {
            assert_eq!(p.connections, 50);
            assert!((p.msg_rate_per_conn - 100.0).abs() < 1e-9);
            assert!(matches!(
                p.correlate,
                zerobench_core::plan::CorrelateStrategy::MonotonicIdPrepend
            ));
        }
        _ => panic!("expected WsEchoRtt"),
    }
}

#[test]
fn ws_echo_rtt_payload_setter() {
    let src = r#"
        scenario("ws", |s| {
            s.step(ws_echo_rtt("ws://api/chat", 1, 10).payload("hello"));
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::WsEchoRtt(p) => {
            // Payload compiled to a static Template; check via expansion.
            let mut buf = Vec::new();
            let mut rng = zerobench_core::rng::from_entropy();
            let counter = std::rc::Rc::new(std::cell::Cell::new(0));
            let mut ctx = zerobench_core::ExpandCtx {
                rng: &mut rng,
                counter: &counter,
                scenario_vars: &[],
            };
            p.payload.expand_into(&mut buf, &mut ctx);
            assert_eq!(std::str::from_utf8(&buf).unwrap(), "hello");
        }
        _ => panic!("expected WsEchoRtt"),
    }
}

#[test]
fn mixed_protocol_plan_from_rhai() {
    let src = r#"
        scenario("http", |s| {
            s.step(GET("http://api/ping"));
        });
        scenario("sse", |s| {
            s.step(sse_hold("http://api/events", 5, "10s"));
        });
        scenario("ws", |s| {
            s.step(ws_echo_rtt("ws://api/chat", 2, 50));
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    assert_eq!(loaded.plan.scenarios.len(), 3);

    use zerobench_core::plan::Protocol;
    let protocols: Vec<Protocol> = loaded
        .plan
        .scenarios
        .iter()
        .map(|s| s.protocol())
        .collect();
    assert_eq!(protocols, vec![Protocol::Http, Protocol::Sse, Protocol::Ws]);
}

// ---------------------------------------------------------------------------
// D3: protocol-builder knobs — correlate / heartbeat_frame / mode
// ---------------------------------------------------------------------------

#[test]
fn ws_echo_rtt_correlate_pingpong() {
    let src = r#"
        scenario("ws", |s| {
            s.step(ws_echo_rtt("ws://api/chat", 1, 10).correlate("pingpong"));
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::WsEchoRtt(p) => {
            assert!(matches!(
                p.correlate,
                zerobench_core::plan::CorrelateStrategy::PingPong
            ));
        }
        _ => panic!("expected WsEchoRtt"),
    }
}

#[test]
fn ws_echo_rtt_correlate_substring() {
    let src = r#"
        scenario("ws", |s| {
            s.step(
                ws_echo_rtt("ws://api/chat", 1, 10)
                    .correlate("substring:zb-echo-")
            );
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::WsEchoRtt(p) => match &p.correlate {
            zerobench_core::plan::CorrelateStrategy::PayloadSubstring { marker } => {
                assert_eq!(marker, "zb-echo-");
            }
            other => panic!("expected PayloadSubstring, got {other:?}"),
        },
        _ => panic!("expected WsEchoRtt"),
    }
}

#[test]
fn ws_echo_rtt_correlate_unknown_rejected() {
    let src = r#"
        scenario("ws", |s| {
            s.step(ws_echo_rtt("ws://api/chat", 1, 10).correlate("nope"));
        });
        duration("10s");
    "#;
    assert!(
        load_script_str(src).is_err(),
        "unknown correlate strategy must fail at script-eval time"
    );
}

#[test]
fn ws_hold_heartbeat_frame_text() {
    let src = r#"
        scenario("ws", |s| {
            s.step(ws_hold("ws://api/bus", 1, "1s").heartbeat_frame("text"));
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::WsHold(p) => {
            assert!(matches!(
                p.heartbeat_frame,
                zerobench_core::plan::HeartbeatFrame::TextApp
            ));
        }
        _ => panic!("expected WsHold"),
    }
}

#[test]
fn sse_fanout_mode_timestamp_default_field() {
    let src = r#"
        scenario("fan", |s| {
            s.step(
                sse_fanout("http://api/events", 3, "5s")
                    .trigger_url("http://api/broadcast")
                    .mode("timestamp")
            );
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::SseFanout(p) => match &p.mode {
            zerobench_core::plan::FanoutMode::Timestamp { emit_field } => {
                assert_eq!(emit_field, "emit_ns");
            }
            other => panic!("expected Timestamp, got {other:?}"),
        },
        _ => panic!("expected SseFanout"),
    }
}

#[test]
fn sse_fanout_mode_timestamp_custom_field() {
    let src = r#"
        scenario("fan", |s| {
            s.step(
                sse_fanout("http://api/events", 3, "5s")
                    .trigger_url("http://api/broadcast")
                    .mode("timestamp:server_ts")
            );
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::SseFanout(p) => match &p.mode {
            zerobench_core::plan::FanoutMode::Timestamp { emit_field } => {
                assert_eq!(emit_field, "server_ts");
            }
            other => panic!("expected Timestamp, got {other:?}"),
        },
        _ => panic!("expected SseFanout"),
    }
}

// ---------------------------------------------------------------------------
// D4: top-level plan metadata setters (cooldown / runs / threads / plan_name)
// ---------------------------------------------------------------------------

#[test]
fn top_level_cooldown_runs_threads_plan_name_are_honoured() {
    let src = r#"
        plan_name("chat-burst");
        runs(3);
        threads(8);
        cooldown("5s");
        scenario("http", |s| { s.step(GET("http://api/ping")); });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    assert_eq!(loaded.plan.name, "chat-burst");
    assert_eq!(loaded.plan.runs, 3);
    assert_eq!(loaded.plan.threads, 8);
    assert_eq!(loaded.plan.cooldown, std::time::Duration::from_secs(5));
}

#[test]
fn defaults_match_single_run_zero_cooldown() {
    // When the script omits the new setters, behaviour should match
    // pre-D4 (runs=1, threads=1, cooldown=ZERO, name="").
    let src = r#"
        scenario("http", |s| { s.step(GET("http://api/ping")); });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    assert_eq!(loaded.plan.runs, 1);
    assert_eq!(loaded.plan.threads, 1);
    assert_eq!(loaded.plan.cooldown, std::time::Duration::ZERO);
    assert!(loaded.plan.name.is_empty());
}

#[test]
fn runs_clamped_to_minimum_one() {
    // runs(0) or runs(-5) must not produce a zero-run plan — the
    // dispatcher would skip the scenario entirely.
    let src = r#"
        runs(0);
        scenario("http", |s| { s.step(GET("http://api/ping")); });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    assert_eq!(loaded.plan.runs, 1);
}

#[test]
fn ws_fanout_heartbeat_frame_and_mode() {
    let src = r#"
        scenario("fan", |s| {
            s.step(
                ws_fanout("ws://api/bus", 2, "5s")
                    .trigger_url("http://api/broadcast")
                    .heartbeat_frame("text")
                    .mode("timestamp")
            );
        });
        duration("10s");
    "#;
    let loaded = load_script_str(src).expect("load");
    match &loaded.plan.scenarios[0].steps[0] {
        Step::WsFanout(p) => {
            assert!(matches!(
                p.subscribers.heartbeat_frame,
                zerobench_core::plan::HeartbeatFrame::TextApp
            ));
            assert!(matches!(
                p.mode,
                zerobench_core::plan::FanoutMode::Timestamp { .. }
            ));
        }
        _ => panic!("expected WsFanout"),
    }
}
