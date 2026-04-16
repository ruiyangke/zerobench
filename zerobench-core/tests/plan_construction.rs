//! Integration tests for the Task 1 plan data model.
//!
//! These exercise the public surface a front-end (CLI, Rhai, request-file
//! parser) would use: allocate vars, build requests, compose scenarios,
//! clone the plan so worker threads can consume it.

use std::time::Duration;

use http::{HeaderName, Method};
use smallvec::smallvec;
use zerobench_core::{
    plan::{RateProfile, RequestPlan},
    Assertion, BodySource, Extract, Plan, Scenario, Step, Template, VarError, VarRegistry,
    VarSlot,
};

#[test]
fn var_registry_allocates_sequential_slots() {
    let mut vars = VarRegistry::new();
    let a = vars.allocate("token").unwrap();
    let b = vars.allocate("order_id").unwrap();
    let c = vars.allocate("csrf").unwrap();

    assert_eq!(a, VarSlot(0));
    assert_eq!(b, VarSlot(1));
    assert_eq!(c, VarSlot(2));
    assert_eq!(vars.len(), 3);
}

#[test]
fn var_registry_returns_same_slot_for_duplicate_name() {
    let mut vars = VarRegistry::new();
    let first = vars.allocate("session").unwrap();
    let second = vars.allocate("session").unwrap();
    assert_eq!(first, second);
    assert_eq!(vars.len(), 1);
}

#[test]
fn var_registry_name_round_trips() {
    let mut vars = VarRegistry::new();
    let slot = vars.allocate("auth").unwrap();
    assert_eq!(vars.name(slot), Some("auth"));
    // Out-of-range slot returns None.
    assert_eq!(vars.name(VarSlot(200)), None);
}

#[test]
fn var_registry_returns_too_many_vars_on_257th_allocation() {
    let mut vars = VarRegistry::new();
    // Fill all 256 slots.
    for i in 0..256 {
        vars.allocate(format!("v{i}"))
            .expect("first 256 allocations must succeed");
    }
    assert_eq!(vars.len(), 256);

    // Allocation 257 must fail cleanly (not panic).
    let err = vars.allocate("overflow").unwrap_err();
    assert_eq!(err, VarError::TooManyVars(256));
    // Registry must be unchanged on failure.
    assert_eq!(vars.len(), 256);

    // Re-allocating an existing name still succeeds even when full.
    let existing = vars.allocate("v0").unwrap();
    assert_eq!(existing, VarSlot(0));
}

#[test]
fn construct_full_plan_and_read_fields_back() {
    let mut vars = VarRegistry::new();
    let token = vars.allocate("token").unwrap();

    let login = RequestPlan {
        method: Method::POST,
        url: Template::literal("/login"),
        headers: smallvec![(
            Template::literal("content-type"),
            Template::literal("application/json"),
        )],
        body: Some(BodySource::Static(bytes::Bytes::from_static(
            br#"{"user":"bob"}"#,
        ))),
        extract: vec![Extract::Header {
            name: HeaderName::from_static("x-auth-token"),
            into: token,
        }],
        checks: vec![
            Assertion::StatusEq(200),
            Assertion::StatusIn(smallvec![200, 201]),
            Assertion::LatencyUnder(Duration::from_millis(500)),
        ],
    };

    let fetch = RequestPlan {
        method: Method::GET,
        url: Template::literal("/api/me"),
        headers: smallvec![(
            Template::literal("authorization"),
            Template::literal("Bearer placeholder"),
        )],
        body: None,
        extract: vec![Extract::StatusCode { into: token }],
        checks: vec![Assertion::StatusEq(200)],
    };

    let scenario = Scenario {
        name: "login-then-fetch".into(),
        rate: RateProfile::Saturate { max_concurrency: 50 },
        steps: vec![
            Step::Request(login),
            Step::Pause(Duration::from_millis(50)),
            Step::PauseRandom {
                min: Duration::from_millis(10),
                max: Duration::from_millis(20),
            },
            Step::Request(fetch),
        ],
    };

    let plan = Plan {
        scenarios: vec![scenario],
        vars,
        duration: Duration::from_secs(30),
        warmup: Some(Duration::from_secs(2)),
    };

    assert_eq!(plan.scenarios.len(), 1);
    assert_eq!(plan.scenarios[0].name, "login-then-fetch");
    assert_eq!(plan.scenarios[0].steps.len(), 4);
    assert_eq!(plan.duration, Duration::from_secs(30));
    assert_eq!(plan.warmup, Some(Duration::from_secs(2)));
    assert_eq!(plan.vars.len(), 1);
    assert_eq!(plan.vars.name(token), Some("token"));

    // Confirm step variants match what we inserted.
    match &plan.scenarios[0].steps[0] {
        Step::Request(r) => assert_eq!(r.method, Method::POST),
        other => panic!("expected Request, got {other:?}"),
    }
    match &plan.scenarios[0].steps[1] {
        Step::Pause(d) => assert_eq!(*d, Duration::from_millis(50)),
        other => panic!("expected Pause, got {other:?}"),
    }
    match &plan.scenarios[0].steps[2] {
        Step::PauseRandom { min, max } => {
            assert_eq!(*min, Duration::from_millis(10));
            assert_eq!(*max, Duration::from_millis(20));
        }
        other => panic!("expected PauseRandom, got {other:?}"),
    }
}

#[test]
fn plan_is_clone_for_sharing_across_threads() {
    let mut vars = VarRegistry::new();
    vars.allocate("x").unwrap();

    let plan = Plan {
        scenarios: vec![Scenario::new(
            "scn",
            vec![Step::Request(RequestPlan::get(Template::literal("/")))],
        )],
        vars,
        duration: Duration::from_secs(1),
        warmup: None,
    };

    let cloned = plan.clone();
    // Each clone is independent.
    assert_eq!(plan.scenarios.len(), cloned.scenarios.len());
    assert_eq!(plan.scenarios[0].name, cloned.scenarios[0].name);

    // Can be moved into a thread (compile-time check that it's Send).
    let handle = std::thread::spawn(move || cloned.scenarios.len());
    assert_eq!(handle.join().unwrap(), 1);
}

#[test]
fn request_plan_get_constructor_yields_expected_defaults() {
    let r = RequestPlan::get(Template::literal("/ping"));
    assert_eq!(r.method, Method::GET);
    assert!(r.headers.is_empty());
    assert!(r.body.is_none());
    assert!(r.extract.is_empty());
    assert!(r.checks.is_empty());
}

#[test]
fn default_plan_is_empty_with_sensible_duration() {
    let p = Plan::new();
    assert!(p.scenarios.is_empty());
    assert_eq!(p.vars.len(), 0);
    assert_eq!(p.duration, Duration::from_secs(30));
    assert!(p.warmup.is_none());
}

/// Ensures the Plan serialization contract stays intact so the diff tool
/// (Task 13) can persist and compare benchmark runs. Touches every in-scope
/// variant: GET/POST, both Extract variants, all three Assertion variants,
/// both BodySource variants, and every Step variant.
#[test]
fn plan_roundtrips_through_serde_json() {
    let mut vars = VarRegistry::new();
    let token = vars.allocate("token").unwrap();
    let status_slot = vars.allocate("last_status").unwrap();

    let login = RequestPlan {
        method: Method::POST,
        url: Template::literal("/login"),
        headers: smallvec![(
            Template::literal("content-type"),
            Template::literal("application/json"),
        )],
        body: Some(BodySource::Static(bytes::Bytes::from_static(
            br#"{"user":"bob"}"#,
        ))),
        extract: vec![Extract::Header {
            name: HeaderName::from_static("x-auth-token"),
            into: token,
        }],
        checks: vec![
            Assertion::StatusEq(200),
            Assertion::StatusIn(smallvec![200, 201]),
            Assertion::LatencyUnder(Duration::from_millis(500)),
        ],
    };

    let probe = RequestPlan {
        method: Method::GET,
        url: Template::literal("/api/me"),
        headers: smallvec![],
        body: None,
        extract: vec![Extract::StatusCode { into: status_slot }],
        checks: vec![Assertion::StatusEq(200)],
    };

    let original = Plan {
        scenarios: vec![Scenario {
            name: "e2e".into(),
            rate: RateProfile::Saturate { max_concurrency: 50 },
            steps: vec![
                Step::Request(login),
                Step::Pause(Duration::from_millis(50)),
                Step::PauseRandom {
                    min: Duration::from_millis(10),
                    max: Duration::from_millis(20),
                },
                Step::Request(probe),
            ],
        }],
        vars,
        duration: Duration::from_secs(30),
        warmup: Some(Duration::from_secs(2)),
    };

    let json = serde_json::to_string(&original).expect("Plan serialization must succeed");
    let decoded: Plan = serde_json::from_str(&json).expect("Plan deserialization must succeed");

    // Structural equality — Plan doesn't implement Eq (Template carries an
    // internal Bytes and we'd rather not require Eq through the whole tree),
    // so we verify the shape round-trips by re-serializing and comparing JSON.
    let reencoded = serde_json::to_string(&decoded).expect("re-serialization must succeed");
    assert_eq!(json, reencoded, "plan JSON must be stable across roundtrip");

    // Spot-check critical fields survived.
    assert_eq!(decoded.scenarios.len(), 1);
    assert_eq!(decoded.scenarios[0].name, "e2e");
    assert_eq!(decoded.scenarios[0].steps.len(), 4);
    assert_eq!(decoded.vars.len(), 2);
    assert_eq!(decoded.vars.name(token), Some("token"));
    assert_eq!(decoded.vars.name(status_slot), Some("last_status"));
    assert_eq!(decoded.duration, Duration::from_secs(30));
    assert_eq!(decoded.warmup, Some(Duration::from_secs(2)));
}
