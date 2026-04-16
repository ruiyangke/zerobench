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
    assert_eq!(scn.steps.len(), 4);

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
    // Step 2: pause
    match &scn.steps[2] {
        Step::Pause(d) => assert_eq!(*d, std::time::Duration::from_millis(20)),
        other => panic!("step 2: expected Pause, got {other:?}"),
    }
    // Step 3: GET /api/feed
    match &scn.steps[3] {
        Step::Request(r) => assert_eq!(r.method, Method::GET),
        other => panic!("step 3: expected Request, got {other:?}"),
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
fn pause_random_step_compiles() {
    let src = r#"
        scenario("x", |s| {
            s.step(pause_random("10ms", "20ms"));
            s.step(GET("http://h:1/"));
        });
        duration("5s");
    "#;
    let loaded = load_script_str(src).unwrap();
    match &loaded.plan.scenarios[0].steps[0] {
        Step::PauseRandom { min, max } => {
            assert_eq!(*min, std::time::Duration::from_millis(10));
            assert_eq!(*max, std::time::Duration::from_millis(20));
        }
        other => panic!("expected PauseRandom, got {other:?}"),
    }
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
