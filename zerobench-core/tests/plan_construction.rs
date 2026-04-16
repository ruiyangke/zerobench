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
    Assertion, BodySource, Extract, Plan, Scenario, Step, Template, VarRegistry, VarSlot,
};

#[test]
fn var_registry_allocates_sequential_slots() {
    let mut vars = VarRegistry::new();
    let a = vars.allocate("token");
    let b = vars.allocate("order_id");
    let c = vars.allocate("csrf");

    assert_eq!(a, VarSlot(0));
    assert_eq!(b, VarSlot(1));
    assert_eq!(c, VarSlot(2));
    assert_eq!(vars.len(), 3);
}

#[test]
fn var_registry_returns_same_slot_for_duplicate_name() {
    let mut vars = VarRegistry::new();
    let first = vars.allocate("session");
    let second = vars.allocate("session");
    assert_eq!(first, second);
    assert_eq!(vars.len(), 1);
}

#[test]
fn var_registry_name_round_trips() {
    let mut vars = VarRegistry::new();
    let slot = vars.allocate("auth");
    assert_eq!(vars.name(slot), Some("auth"));
    // Out-of-range slot returns None.
    assert_eq!(vars.name(VarSlot(200)), None);
}

#[test]
fn construct_full_plan_and_read_fields_back() {
    let mut vars = VarRegistry::new();
    let token = vars.allocate("token");

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
        rate: RateProfile::Placeholder,
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
    vars.allocate("x");

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
