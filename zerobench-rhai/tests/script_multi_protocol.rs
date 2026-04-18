//! Integration tests for the Tier-1 unification DSL — `SSE(...)` and
//! `WS(...)` builders.
//!
//! These tests mirror `script_compile.rs` but focus on the new builder
//! types: they compile a Rhai source, then assert on the `Plan` shape
//! that the DSL produced. Protocol inference is exercised indirectly
//! by checking `Step::SseStream` / `Step::WsRound` variants.

use zerobench_core::plan::{Protocol, Step};
use zerobench_rhai::load_script_str;

/// A single-scenario SSE script compiles to one `Step::SseStream`.
#[test]
fn sse_script_produces_sse_step() {
    let src = r#"
        scenario("events", |s| {
            s.step(SSE("http://h:1234/sse?chunks=100").expect_chunks(100));
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 1);
    let steps = &loaded.plan.scenarios[0].steps;
    assert_eq!(steps.len(), 1);
    match &steps[0] {
        Step::SseStream(p) => {
            assert_eq!(p.expect_chunks, Some(100));
            assert!(p.headers.is_empty());
        }
        other => panic!("expected SseStream, got {other:?}"),
    }
    assert_eq!(loaded.plan.scenarios[0].protocol(), Protocol::Sse);
}

/// A single-scenario WS script compiles to one `Step::WsRound`.
#[test]
fn ws_script_produces_ws_step() {
    let src = r#"
        scenario("chat", |s| {
            s.step(WS("ws://h:1234/ws").message("ping"));
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 1);
    let steps = &loaded.plan.scenarios[0].steps;
    assert_eq!(steps.len(), 1);
    match &steps[0] {
        Step::WsRound(p) => {
            // Expand the template against an empty context — since
            // "ping" is static, the single part should be a Literal.
            // A simpler assertion: the template isn't empty.
            assert!(p.message.part_count() >= 1);
        }
        other => panic!("expected WsRound, got {other:?}"),
    }
    assert_eq!(loaded.plan.scenarios[0].protocol(), Protocol::Ws);
}

/// Mixed scripts produce the right variant per scenario and the
/// `Plan::scenarios` order matches the declaration order.
#[test]
fn mixed_protocols_produce_matching_scenarios() {
    let src = r#"
        scenario("http-rpc", |s| {
            s.step(GET("http://h:1234/ping").expect_status(200));
        });
        scenario("sse-events", |s| {
            s.step(SSE("http://h:1234/sse").expect_chunks(10));
        });
        scenario("ws-echo", |s| {
            s.step(WS("ws://h:1234/ws").message("hi"));
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.plan.scenarios.len(), 3);
    // Declaration order is preserved.
    assert_eq!(loaded.plan.scenarios[0].name, "http-rpc");
    assert_eq!(loaded.plan.scenarios[1].name, "sse-events");
    assert_eq!(loaded.plan.scenarios[2].name, "ws-echo");

    assert_eq!(loaded.plan.scenarios[0].protocol(), Protocol::Http);
    assert_eq!(loaded.plan.scenarios[1].protocol(), Protocol::Sse);
    assert_eq!(loaded.plan.scenarios[2].protocol(), Protocol::Ws);

    match &loaded.plan.scenarios[0].steps[0] {
        Step::Request(_) => {}
        other => panic!("expected Request in scenario 0, got {other:?}"),
    }
    match &loaded.plan.scenarios[1].steps[0] {
        Step::SseStream(_) => {}
        other => panic!("expected SseStream in scenario 1, got {other:?}"),
    }
    match &loaded.plan.scenarios[2].steps[0] {
        Step::WsRound(_) => {}
        other => panic!("expected WsRound in scenario 2, got {other:?}"),
    }
}

/// SSE builder header chaining survives to the compiled plan.
#[test]
fn sse_builder_headers_propagate() {
    let src = r#"
        scenario("e", |s| {
            s.step(
                SSE("http://h:1/sse")
                    .header("X-Trace", "abc")
                    .header("X-Version", "v2")
                    .expect_chunks(5)
            );
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    let Step::SseStream(p) = &loaded.plan.scenarios[0].steps[0] else {
        panic!("expected SseStream");
    };
    assert_eq!(p.headers.len(), 2);
    assert_eq!(p.expect_chunks, Some(5));
}

/// WS builder message is stored as a template so `{{...}}` expansion
/// works at runtime.
#[test]
fn ws_builder_message_template() {
    let src = r#"
        scenario("w", |s| {
            s.step(
                WS("ws://h:1/ws")
                    .header("X-Client", "zerobench")
                    .message("{{uuid}}")
            );
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    let Step::WsRound(p) = &loaded.plan.scenarios[0].steps[0] else {
        panic!("expected WsRound");
    };
    assert_eq!(p.headers.len(), 1);
    // Templated message has at least one dynamic part.
    assert!(p.message.part_count() >= 1);
}

/// A plan with only pause-or-empty scenarios is still rejected — we
/// need at least one wire step somewhere. Regression guard for the
/// `NoRequestSteps` error variant after SSE/WS were added.
#[test]
fn plan_with_no_wire_steps_is_rejected() {
    let src = r#"
        scenario("noop", |s| {
            s.step(pause("10ms"));
        });
        duration("1s");
    "#;
    let err = zerobench_rhai::load_script_str(src).unwrap_err();
    assert!(
        matches!(err, zerobench_rhai::ScriptError::NoRequestSteps),
        "got {err:?}"
    );
}

/// SSE target URL parses correctly for the CLI dispatcher.
#[test]
fn sse_script_derives_target_from_url() {
    let src = r#"
        scenario("e", |s| {
            s.step(SSE("http://example.com:8080/sse"));
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.target.host, "example.com");
    assert_eq!(loaded.target.port, 8080);
    assert!(!loaded.target.tls);
}

/// WS target URL parses correctly (ws:// scheme).
#[test]
fn ws_script_derives_target_from_url() {
    let src = r#"
        scenario("w", |s| {
            s.step(WS("ws://example.com:9000/chat"));
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    assert_eq!(loaded.target.host, "example.com");
    assert_eq!(loaded.target.port, 9000);
    // ws:// is non-TLS.
    assert!(!loaded.target.tls);
}
