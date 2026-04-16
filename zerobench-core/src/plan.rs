//! The Phase 1 data model.
//!
//! A [`Plan`] is the frozen, thread-shareable description of what the engine
//! should execute. CLI, request-file, and Rhai front-ends all compile down
//! to this type. Phase 2 (rate scheduler → dispatcher → transport) consumes
//! a `Plan` and never inspects the original source.
//!
//! See `docs/design.md` §3 for the full data model. This module implements
//! the v0.0.1 subset: Task 1 lands the skeleton; later tasks extend
//! [`BodySource`], [`Extract`], [`Assertion`] with richer variants, and
//! swap [`RateProfile`] from a placeholder into a real scheduler input.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderName, Method};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::template::Template;
use crate::var::{VarRegistry, VarSlot};

/// A complete benchmark plan.
///
/// Cheap to [`Clone`] — every field is either a small owned value or a
/// reference-counted buffer. Workers receive their own clones and never
/// mutate the plan during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// Scenarios to run, each with its own rate profile and steps. The
    /// engine runs all scenarios concurrently.
    pub scenarios: Vec<Scenario>,
    /// Compile-time variable slot allocation — the registry's order fixes
    /// the meaning of every [`VarSlot`] in this plan.
    pub vars: VarRegistry,
    /// Total measurement duration. Warmup is in addition to this.
    pub duration: Duration,
    /// Optional warmup phase — requests are fired but stats discarded.
    pub warmup: Option<Duration>,
}

impl Plan {
    /// Fresh empty plan with a default 30s duration and no warmup.
    pub fn new() -> Self {
        Self {
            scenarios: Vec::new(),
            vars: VarRegistry::new(),
            duration: Duration::from_secs(30),
            warmup: None,
        }
    }
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

/// One named traffic stream — a sequence of steps executed top-to-bottom
/// per iteration, emitted at the scenario's [`RateProfile`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    /// Human-readable name used in reports and error messages.
    pub name: String,
    /// Placeholder until Task 10 lands the real rate-profile type. Filled
    /// in with a single-variant enum so type sites compile unchanged.
    pub rate: RateProfile,
    /// Steps executed in order per iteration; on error, execution of the
    /// remaining steps for that iteration is skipped.
    pub steps: Vec<Step>,
}

impl Scenario {
    /// Construct a scenario with the given name and steps; rate profile
    /// starts as the placeholder variant until Task 10.
    pub fn new(name: impl Into<String>, steps: Vec<Step>) -> Self {
        Self {
            name: name.into(),
            rate: RateProfile::Placeholder,
            steps,
        }
    }
}

/// Placeholder type; Task 10 replaces this with `Constant`/`Ramp`/`Stepped`
/// variants and per-scenario scheduler wiring. Keeping the enum shape
/// stable now means the [`Scenario`] field type never has to change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RateProfile {
    /// Fill-in variant so the type has a non-zero-sized discriminant and
    /// serialization works before the real profile lands.
    Placeholder,
}

/// One unit of work inside a scenario iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Step {
    /// Send an HTTP request and optionally extract/assert on the response.
    Request(RequestPlan),
    /// Sleep a fixed duration before the next step.
    Pause(Duration),
    /// Sleep a uniformly-random duration in `[min, max]`.
    PauseRandom { min: Duration, max: Duration },
}

/// A single request's compiled description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPlan {
    /// HTTP method (GET/POST/...).
    #[serde(with = "http_serde::method")]
    pub method: Method,
    /// Target URL — a template so `{{env:HOST}}/api/{{var:id}}` works.
    pub url: Template,
    /// Header name/value pairs. Both sides are templates because cookies
    /// and auth tokens frequently interpolate extracted vars.
    pub headers: SmallVec<[(Template, Template); 8]>,
    /// Optional body source. `None` = empty body.
    pub body: Option<BodySource>,
    /// Response extractors applied after the body is received; they write
    /// into [`VarSlot`]s declared in the plan's registry.
    pub extract: Vec<Extract>,
    /// Post-response assertions. Failure increments
    /// `errors.assertion_failed` but does not abort the scenario.
    pub checks: Vec<Assertion>,
}

impl RequestPlan {
    /// Construct a minimal GET request against the given URL template.
    /// Headers/body/extract/checks start empty.
    pub fn get(url: Template) -> Self {
        Self {
            method: Method::GET,
            url,
            headers: SmallVec::new(),
            body: None,
            extract: Vec::new(),
            checks: Vec::new(),
        }
    }
}

/// How the request body is produced per iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BodySource {
    /// Pre-encoded static bytes — no `{{...}}` in the source, so we skip
    /// the template engine entirely.
    Static(Bytes),
    /// Template expanded per iteration into a scratch buffer.
    Template(Template),
    // File / FilePool variants land with the request-file parser (Task 11).
}

/// Post-response extraction. Written value is stored in the scenario
/// context at the named slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Extract {
    /// Copy a response header value into `into`. Missing header → slot set
    /// to `None`.
    Header {
        #[serde(with = "http_serde::header_name")]
        name: HeaderName,
        into: VarSlot,
    },
    /// Write the numeric status code (as ASCII decimal bytes) into `into`.
    StatusCode { into: VarSlot },
    // JsonPath / RegexBody variants land in later tasks.
}

/// Post-response check. Failure is recorded but non-fatal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Assertion {
    /// Exact status-code match.
    StatusEq(u16),
    /// Status must equal one of these codes.
    StatusIn(SmallVec<[u16; 4]>),
    /// Total request latency must be below this duration.
    LatencyUnder(Duration),
    // BodyContains / JsonEq variants land with richer assertions.
}

// ---------------------------------------------------------------------------
// http_serde — inline, tiny helpers so we don't need a new workspace dep.
// ---------------------------------------------------------------------------
//
// `http::Method` and `http::HeaderName` don't implement serde by default.
// Instead of pulling the whole `http-serde` crate, we inline the narrow
// helpers we need. Phase 1 plans only ever round-trip through JSON for
// the diff tool (Task 13) and debug logging; this is adequate.

mod http_serde {
    pub mod method {
        use http::Method;
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        pub fn serialize<S: Serializer>(m: &Method, s: S) -> Result<S::Ok, S::Error> {
            m.as_str().serialize(s)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Method, D::Error> {
            let s = <&str>::deserialize(d)?;
            s.parse::<Method>().map_err(serde::de::Error::custom)
        }
    }

    pub mod header_name {
        use http::HeaderName;
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        pub fn serialize<S: Serializer>(n: &HeaderName, s: S) -> Result<S::Ok, S::Error> {
            n.as_str().serialize(s)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<HeaderName, D::Error> {
            let s = <String>::deserialize(d)?;
            HeaderName::from_bytes(s.as_bytes()).map_err(serde::de::Error::custom)
        }
    }
}
