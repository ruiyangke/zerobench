//! Command-line argument parsing via `clap` derive.
//!
//! Parsing produces a [`CliArgs`]; the conversion to a runnable
//! [`zerobench_core::plan::Plan`] / [`zerobench_core::transport::Target`] /
//! [`zerobench_core::transport::TransportOpts`] triple lives in
//! [`crate::plan_from_cli`] so this file stays a thin, declarative
//! schema.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Parser, ValueEnum};

/// Return the number of available CPU cores, falling back to 1 if the
/// query fails (e.g. inside a restricted container).
pub fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

// ---------------------------------------------------------------------------
// `--version` long form — build profile baked in at compile time.
// ---------------------------------------------------------------------------

#[cfg(feature = "tui")]
const FEAT_TUI: &str = ", tui";
#[cfg(not(feature = "tui"))]
const FEAT_TUI: &str = "";

#[cfg(debug_assertions)]
const BUILD_PROFILE: &str = "debug";
#[cfg(not(debug_assertions))]
const BUILD_PROFILE: &str = "release";

/// Compose the long version string at first call and leak into a
/// `&'static str` clap can own. We can't `concat!` across non-literal
/// `const` references, so we build it at startup instead. The leak is
/// a tiny one-time allocation for the lifetime of the process, which
/// is acceptable for a CLI tool.
///
/// HTTP/1, HTTP/2, SSE, and WebSocket are always-on — they don't appear
/// in the feature list because there's no way to build `zerobench`
/// without them.
pub fn long_version() -> &'static str {
    use std::sync::OnceLock;
    static S: OnceLock<&'static str> = OnceLock::new();
    S.get_or_init(|| {
        let s = format!(
            "{ver}\nProtocols: http/1, http/2, sse, websocket\nFeatures: default{tui}\nBuild: {prof}, mio/epoll",
            ver = env!("CARGO_PKG_VERSION"),
            tui = FEAT_TUI,
            prof = BUILD_PROFILE,
        );
        Box::leak(s.into_boxed_str())
    })
}

/// `zerobench` — fast, correct HTTP benchmarking tool.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "zerobench",
    version,
    long_version = long_version(),
    about = "HTTP/1, HTTP/2, WebSocket, and SSE benchmarking — open-loop, HDR-precise, mio/epoll",
    after_help = "EXAMPLES:\n  \
        zerobench http://localhost:8080                         # 30s saturate, 50 conns\n  \
        zerobench http://localhost:8080 -d 1m -c 200            # 1 minute, 200 conns\n  \
        zerobench http://localhost:8080 -r 10k --tui            # open-loop 10k req/s + live dashboard\n  \
        zerobench https://api.example.com -H 'Auth: x' --json '{\"k\":1}'\n  \
        zerobench --sse http://localhost:8080/events -c 100     # SSE stream bench\n  \
        zerobench --ws   ws://localhost:8080/chat  -c 100       # WebSocket bench\n  \
        zerobench diff baseline.json current.json               # regression diff\n\
        \n\
        Full help: `zerobench --help`. Docs: https://github.com/ruiyangke/zerobench\
        "
)]
pub struct CliArgs {
    /// Optional sub-command (`diff`, ...). When omitted, the positional
    /// `url` / flags describe a bench run.
    #[command(subcommand)]
    pub command: Option<Subcommand>,

    /// Target URL. Optional when `--request-file` or `--requests` is
    /// given — the URL is derived from the file's Host header.
    #[arg(conflicts_with_all = ["request_file", "requests"])]
    pub url: Option<String>,

    // ---------- Input ----------

    /// Parse a single `.http` request file (curl `--trace-ascii` format).
    #[arg(long = "request-file", conflicts_with = "requests",
          help_heading = "Input")]
    pub request_file: Option<PathBuf>,

    /// Parse every `*.http` in DIR as a scenario. Weights via
    /// `scenarios.toml`; equal when absent.
    #[arg(long = "requests", conflicts_with = "request_file",
          help_heading = "Input")]
    pub requests: Option<PathBuf>,

    // ---------- Load ----------

    /// OS worker threads (each has its own mio poll). Default: CPU cores.
    #[arg(short = 't', long = "threads", default_value_t = num_cpus(),
          help_heading = "Load")]
    pub threads: usize,

    /// Max concurrent connections (H1) or streams (H2).
    #[arg(short = 'c', long = "connections", default_value_t = 50,
          help_heading = "Load")]
    pub connections: usize,

    /// Measurement duration (`10s`, `1m`, `2m30s`).
    #[arg(short = 'd', long = "duration", default_value = "30s",
          value_parser = parse_duration_flag,
          help_heading = "Load")]
    pub duration: Duration,

    /// Closed-loop saturate mode — N workers looping request-then-response.
    /// Optional — this is the default when `--rate` is not given.
    #[arg(long = "saturate", action = ArgAction::SetTrue,
          conflicts_with = "rate",
          help_heading = "Load")]
    pub saturate: bool,

    /// Open-loop target rate in req/s (`100`, `10k`, `1.5k`).
    /// Mutually exclusive with `--saturate`.
    #[arg(short = 'r', long = "rate", value_parser = parse_rate_flag,
          help_heading = "Load")]
    pub rate: Option<f64>,

    /// Warmup phase before measurement (e.g. `5s`). Requests are fired
    /// but stats discarded. Mio dispatch does not honour this yet (TODO).
    #[arg(long = "warmup", value_parser = parse_duration_flag,
          help_heading = "Load")]
    pub warmup: Option<Duration>,

    // ---------- Request ----------

    /// HTTP method (default: GET; promoted to POST when a body is given).
    #[arg(short = 'X', long = "method",
          help_heading = "Request")]
    pub method: Option<String>,

    /// Add a header. Repeat for multiple. Form: `Name: Value`.
    #[arg(short = 'H', long = "header", value_parser = parse_header_flag,
          help_heading = "Request")]
    pub headers: Vec<(String, String)>,

    /// Inline request body. Implies POST unless `-X` was given.
    #[arg(long = "body",
          conflicts_with_all = ["body_file", "json", "form"],
          help_heading = "Request")]
    pub body: Option<String>,

    /// Request body from a file path.
    #[arg(long = "body-file",
          conflicts_with_all = ["json", "form"],
          help_heading = "Request")]
    pub body_file: Option<PathBuf>,

    /// JSON body. Sets `Content-Type: application/json` and implies POST.
    #[arg(long = "json", conflicts_with = "form",
          help_heading = "Request")]
    pub json: Option<String>,

    /// Form body (`k=v&other=thing`). Sets
    /// `Content-Type: application/x-www-form-urlencoded` and implies POST.
    #[arg(long = "form",
          help_heading = "Request")]
    pub form: Option<String>,

    /// Basic auth `user:pass`. Adds `Authorization: Basic <b64>`.
    /// Explicit `-H Authorization:` wins with a warning.
    #[arg(long = "basic-auth", value_parser = parse_basic_auth_flag,
          conflicts_with = "bearer",
          help_heading = "Request")]
    pub basic_auth: Option<String>,

    /// Bearer token. Adds `Authorization: Bearer <token>`.
    #[arg(long = "bearer",
          help_heading = "Request")]
    pub bearer: Option<String>,

    // ---------- Assertions ----------

    /// Assertion: exact status code.
    #[arg(long = "expect-status",
          help_heading = "Assertions")]
    pub expect_status: Option<u16>,

    /// Assertion: status code in list (e.g. `200,201,204`).
    #[arg(long = "expect-status-in", value_parser = parse_status_list,
          num_args = 1,
          help_heading = "Assertions")]
    pub expect_status_in: Option<StatusList>,

    // ---------- Protocol ----------

    /// Preferred HTTP version. `auto` lets HTTPS ALPN pick; HTTP stays H1.
    #[arg(long = "http-version", value_enum,
          default_value_t = CliHttpVersion::Auto,
          help_heading = "Protocol")]
    pub http_version: CliHttpVersion,

    /// Force HTTP/2 with prior-knowledge framing (equivalent to
    /// `--http-version h2`; our mio_h2 always uses prior knowledge on
    /// plain HTTP).
    #[arg(long = "http2-prior-knowledge", action = ArgAction::SetTrue,
          help_heading = "Protocol")]
    pub http2_prior_knowledge: bool,

    // ---------- Network ----------

    /// TCP+TLS connect timeout.
    #[arg(long = "connect-timeout", default_value = "5s",
          value_parser = parse_duration_flag,
          help_heading = "Network")]
    pub connect_timeout: Duration,

    /// Per-request deadline.
    #[arg(long = "timeout", default_value = "30s",
          value_parser = parse_duration_flag,
          help_heading = "Network")]
    pub request_timeout: Duration,

    /// Accept invalid TLS certificates. No-op for http:// targets.
    #[arg(short = 'k', long = "insecure", action = ArgAction::SetTrue,
          help_heading = "Network")]
    pub insecure: bool,

    /// curl-style DNS override `HOST:PORT:ADDR`. Repeat for multiple.
    /// E.g. `--resolve example.com:443:10.0.0.5`.
    #[arg(long = "resolve", value_parser = parse_resolve_flag,
          help_heading = "Network")]
    pub resolve: Vec<(String, u16, String)>,

    // ---------- Output ----------

    /// Color output preference.
    #[arg(long = "color", value_enum, default_value_t = CliColor::Auto,
          help_heading = "Output")]
    pub color: CliColor,

    /// Output format.
    #[arg(long = "format", value_enum, default_value_t = CliFormat::Terminal,
          help_heading = "Output")]
    pub format: CliFormat,

    /// Live ratatui dashboard during the run. Requires TTY; mutex with
    /// `--format jsonl`.
    #[cfg(feature = "tui")]
    #[arg(long = "tui", action = ArgAction::SetTrue,
          help_heading = "Output")]
    pub tui: bool,

    /// Write final report to FILE instead of stdout. Affects all formats.
    #[arg(short = 'o', long = "output",
          help_heading = "Output")]
    pub output: Option<PathBuf>,

    /// Parse args, build plan, resolve DNS, print the config, exit 0.
    /// No traffic is sent.
    #[arg(long = "dry-run", action = ArgAction::SetTrue,
          help_heading = "Output")]
    pub dry_run: bool,
}

// ---------------------------------------------------------------------------
// Value enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliColor {
    Always,
    Auto,
    Never,
}

/// Surface name for `zerobench_core::HttpVersionPref`.
///
/// Kept as its own enum rather than re-using the core one because clap's
/// `ValueEnum` derive needs to own the type (coherence rules), and we
/// also want the CLI surface to be stable even if the core enum grows
/// new variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliHttpVersion {
    /// Let the transport decide (default).
    Auto,
    /// Force HTTP/1.1.
    H1,
    /// Force HTTP/2. Requires the `h2` feature.
    H2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliFormat {
    /// Human-readable summary (default).
    Terminal,
    /// Full JSON summary written at end-of-run.
    Json,
    /// Stream one JSON line per second during the run; final terminal
    /// summary is written to stderr so stdout stays pure JSONL.
    Jsonl,
    /// Prometheus textfile format emitted at end-of-run (no output
    /// during the run).
    Prom,
}

// ---------------------------------------------------------------------------
// Sub-commands
// ---------------------------------------------------------------------------

/// Sub-commands beyond the default "run a benchmark" behaviour.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum Subcommand {
    /// Compare two JSON bench outputs and report deltas.
    Diff(DiffArgs),
    /// Run a Rhai scenario script. The script is evaluated once at
    /// startup to produce a `Plan`; the Rhai engine is then dropped and
    /// the benchmark runs as pure Rust.
    Run(RunArgs),
    /// Rigorous steady-state measurement (v0.1.0). Runs N consecutive
    /// benchmark runs against `URL` with warmup + cooldown, ships
    /// through the client self-check gate, and archives the result
    /// under `$ZEROBENCH_HOME/runs/<url_fp>/<run_id>/`. Default
    /// 60s × 3 runs with 15s warmup and 10s cooldown.
    Measure(crate::verbs::measure::MeasureArgs),
    /// 5-second smoke test (v0.1.0). Small, opinionated, no archive,
    /// no calibration gate — "does the target respond, roughly how
    /// fast?" For rigorous measurements use `measure`.
    Probe(crate::verbs::probe::ProbeArgs),
    /// Compare two `result.json` files and report percentile deltas.
    Compare(crate::verbs::diff::CompareArgs),
    /// Run the client-side self-check (loopback echo) and print the
    /// ceiling + scheduler jitter. Useful for "what's the fastest
    /// my machine can push?" without a target.
    Calibrate(crate::verbs::calibrate::CalibrateArgs),
    /// Ramp offered rate across --from..--to over --over; report the
    /// (rate, p99) curve and the knee. Per PHILOSOPHY §P4.
    Curve(crate::verbs::curve::CurveArgs),
}

/// Arguments for `zerobench run <script.rhai>`.
///
/// The script is the source of truth for scenarios, rate, duration,
/// warmup, and transport. The flags here are safe overrides for the
/// common "I want to re-run the same script with a longer duration"
/// case — they win over whatever the script said.
#[derive(Debug, Clone, clap::Args)]
pub struct RunArgs {
    /// Path to the `.rhai` script.
    #[arg(value_name = "SCRIPT")]
    pub script: PathBuf,

    /// Override the script's `duration(...)` — useful for quick
    /// smoke-tests of an otherwise-long scenario. Must still be a
    /// positive duration; parsed with the same grammar as the
    /// `--duration` flag on the default subcommand.
    #[arg(long = "duration", value_parser = parse_duration_flag)]
    pub duration: Option<Duration>,

    /// Override the script's `rate(...)` — parsed with the same
    /// grammar as `-r` / `--rate` on the default subcommand.
    /// Implicitly overrides any per-scenario `.rate(...)` too.
    #[arg(long = "rate", short = 'r', value_parser = parse_rate_flag,
          conflicts_with = "saturate")]
    pub rate: Option<f64>,

    /// Override the script's rate/saturate profile with closed-loop
    /// saturate across all scenarios. Useful for running the same
    /// Rhai file in both throughput (`--rate ...`) and tail-latency
    /// (`--saturate`) modes without editing the script.
    #[arg(long = "saturate", action = ArgAction::SetTrue,
          conflicts_with = "rate")]
    pub saturate: bool,

    /// Run scenarios concurrently, sharing the `-c N` connection pool
    /// and interleaving scenario picks per request. By default each
    /// scenario runs serially with the full pool to its own endpoint.
    /// Parallel mode is useful for modelling a realistic mixed-traffic
    /// client fleet under a fixed connection budget.
    #[arg(long = "parallel", action = ArgAction::SetTrue)]
    pub parallel: bool,

    /// Number of OS worker threads.
    #[arg(short = 't', long = "threads", default_value_t = num_cpus())]
    pub threads: usize,

    /// Max concurrent connections. Used in saturate mode (when the
    /// script set `saturate(...)` or neither `rate()` nor `saturate()`
    /// was called) and as the pool ceiling in open-loop mode.
    #[arg(short = 'c', long = "connections", default_value_t = 50)]
    pub connections: usize,

    /// Output format.
    #[arg(long = "format", value_enum, default_value_t = CliFormat::Terminal)]
    pub format: CliFormat,

    /// Color output preference.
    #[arg(long = "color", value_enum, default_value_t = CliColor::Auto)]
    pub color: CliColor,

    /// TCP+TLS connect timeout.
    #[arg(long = "connect-timeout", default_value = "5s",
          value_parser = parse_duration_flag)]
    pub connect_timeout: Duration,

    /// Per-request deadline.
    #[arg(long = "timeout", default_value = "30s",
          value_parser = parse_duration_flag)]
    pub request_timeout: Duration,

    /// Accept invalid TLS certificates. Only meaningful for https://
    /// targets.
    #[arg(short = 'k', long = "insecure", action = ArgAction::SetTrue)]
    pub insecure: bool,

    /// Basic auth `user:pass`. Adds `Authorization: Basic <b64>`.
    #[arg(long = "basic-auth", value_parser = parse_basic_auth_flag,
          conflicts_with = "bearer")]
    pub basic_auth: Option<String>,

    /// Bearer token. Adds `Authorization: Bearer <token>`.
    #[arg(long = "bearer")]
    pub bearer: Option<String>,

    /// Write final report to FILE instead of stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
}

/// Arguments for `zerobench diff`.
#[derive(Debug, Clone, clap::Args)]
pub struct DiffArgs {
    /// Baseline JSON output (from a prior run).
    pub baseline: PathBuf,

    /// Current JSON output (from this run).
    pub current: PathBuf,

    /// p99 regression threshold in percent. A p99 increase above this
    /// fraction counts as a regression. Default 5%. Must be ≥ 0.
    #[arg(
        long = "threshold-p99",
        default_value_t = 5.0,
        value_parser = parse_threshold_flag,
    )]
    pub threshold_p99: f64,

    /// RPS regression threshold in percent. An RPS decrease above this
    /// fraction counts as a regression. Default 2%. Must be ≥ 0.
    #[arg(
        long = "threshold-rps",
        default_value_t = 2.0,
        value_parser = parse_threshold_flag,
    )]
    pub threshold_rps: f64,

    /// Output format for the diff.
    #[arg(long = "format", value_enum, default_value_t = DiffFormat::Terminal)]
    pub format: DiffFormat,

    /// Color output preference.
    #[arg(long = "color", value_enum, default_value_t = CliColor::Auto)]
    pub color: CliColor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DiffFormat {
    /// Human-readable per-metric delta table (default).
    Terminal,
    /// Structured JSON blob with baseline/current/delta per metric and
    /// an overall `regression` flag.
    Json,
}

// ---------------------------------------------------------------------------
// Value parsers
// ---------------------------------------------------------------------------

/// Parse a duration spec (`10s`, `1m`, `2m30s`, `500ms`).
pub fn parse_duration_flag(s: &str) -> Result<Duration, String> {
    parse_duration(s).ok_or_else(|| {
        format!(
            "invalid duration {s:?}; expected forms like `10s`, `1m`, `2m30s`, `500ms`"
        )
    })
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total = Duration::ZERO;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Numeric run (integer only; we don't support fractional seconds).
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if start == i {
            return None;
        }
        let n: u64 = std::str::from_utf8(&bytes[start..i]).ok()?.parse().ok()?;

        // Unit run.
        let u_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() {
            i += 1;
        }
        let unit = std::str::from_utf8(&bytes[u_start..i]).ok()?.trim();
        let d = match unit {
            "ns" => Duration::from_nanos(n),
            "us" | "µs" => Duration::from_micros(n),
            "ms" => Duration::from_millis(n),
            "s" | "" => Duration::from_secs(n),
            "m" => Duration::from_secs(n * 60),
            "h" => Duration::from_secs(n * 3600),
            _ => return None,
        };
        total += d;
    }
    Some(total)
}

/// Parse a rate spec (`100`, `10k`, `1.5k`, `2M`).
pub fn parse_rate_flag(s: &str) -> Result<f64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty rate".into());
    }
    let (num, mult) = match s.chars().last().unwrap() {
        'k' | 'K' => (&s[..s.len() - 1], 1_000.0f64),
        'm' | 'M' => (&s[..s.len() - 1], 1_000_000.0f64),
        _ => (s, 1.0),
    };
    let n: f64 = num
        .parse()
        .map_err(|e| format!("invalid rate {s:?}: {e}"))?;
    if !n.is_finite() || n <= 0.0 {
        return Err(format!("rate must be a positive finite number, got {n}"));
    }
    Ok(n * mult)
}

/// Parse `Name: Value` into a tuple.
pub fn parse_header_flag(s: &str) -> Result<(String, String), String> {
    let (name, value) = s
        .split_once(':')
        .ok_or_else(|| format!("expected 'Name: Value', got {s:?}"))?;
    Ok((name.trim().to_string(), value.trim().to_string()))
}

/// Newtype around `Vec<u16>` so clap treats the parsed output as a
/// single value (rather than a list of repeated occurrences).
#[derive(Debug, Clone)]
pub struct StatusList(pub Vec<u16>);

/// Parse an f64 threshold value, rejecting negatives and non-finite
/// numbers. Thresholds are percent values (e.g. `5.0` for 5%) — a
/// negative tolerance is meaningless and almost certainly a typo.
pub fn parse_threshold_flag(s: &str) -> Result<f64, String> {
    let n: f64 = s
        .parse()
        .map_err(|e| format!("invalid threshold {s:?}: {e}"))?;
    if !n.is_finite() {
        return Err(format!("threshold must be finite, got {n}"));
    }
    if n < 0.0 {
        return Err(format!(
            "threshold must be >= 0 (percent), got {n}"
        ));
    }
    Ok(n)
}

/// Parse a comma-separated list of status codes.
pub fn parse_status_list(s: &str) -> Result<StatusList, String> {
    let out: Result<Vec<u16>, _> = s
        .split(',')
        .map(|t| t.trim().parse::<u16>())
        .collect();
    out.map(StatusList)
        .map_err(|e| format!("invalid status list {s:?}: {e}"))
}

/// Validate `--basic-auth USER:PASS`. Must contain a `:`. Value is kept
/// verbatim — the base64 encode happens in `plan_from_cli` so the
/// parser stays allocation-light.
pub fn parse_basic_auth_flag(s: &str) -> Result<String, String> {
    if s.is_empty() {
        return Err("empty basic-auth value".into());
    }
    if !s.contains(':') {
        return Err(format!(
            "expected 'USER:PASS' for --basic-auth, got {s:?}"
        ));
    }
    Ok(s.to_string())
}

/// Parse a curl-compatible `HOST:PORT:ADDR` resolve override. The HOST
/// and ADDR strings are allowed to be IPv6 literals (bracketed). We
/// split on the final two `:` so `ADDR` can itself be an IPv6 literal
/// like `[::1]`.
pub fn parse_resolve_flag(s: &str) -> Result<(String, u16, String), String> {
    // Find the last two ':' — those separate PORT:ADDR. Everything
    // before is HOST.
    let last = s
        .rfind(':')
        .ok_or_else(|| format!("expected 'HOST:PORT:ADDR' for --resolve, got {s:?}"))?;
    let (head, addr) = s.split_at(last);
    let addr = addr.trim_start_matches(':').to_string();
    let mid = head
        .rfind(':')
        .ok_or_else(|| format!("expected 'HOST:PORT:ADDR' for --resolve, got {s:?}"))?;
    let (host, port_str) = head.split_at(mid);
    let port_str = port_str.trim_start_matches(':');
    let port: u16 = port_str
        .parse()
        .map_err(|_| format!("invalid port in --resolve {s:?}: {port_str:?}"))?;
    if host.is_empty() || addr.is_empty() {
        return Err(format!("empty HOST or ADDR in --resolve {s:?}"));
    }
    Ok((host.to_string(), port, addr))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_duration_forms() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(
            parse_duration("2m30s"),
            Some(Duration::from_secs(150))
        );
        assert_eq!(
            parse_duration("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        // Bare number defaults to seconds.
        assert_eq!(parse_duration("30"), Some(Duration::from_secs(30)));
        // Invalid.
        assert!(parse_duration("").is_none());
        assert!(parse_duration("10x").is_none());
        assert!(parse_duration("abc").is_none());
    }

    #[test]
    fn parse_rate_suffixes() {
        assert_eq!(parse_rate_flag("100"), Ok(100.0));
        assert_eq!(parse_rate_flag("10k"), Ok(10_000.0));
        assert_eq!(parse_rate_flag("1.5k"), Ok(1_500.0));
        assert_eq!(parse_rate_flag("2M"), Ok(2_000_000.0));
        assert!(parse_rate_flag("").is_err());
        assert!(parse_rate_flag("-5").is_err());
        assert!(parse_rate_flag("0").is_err());
    }

    #[test]
    fn parse_header_form() {
        assert_eq!(
            parse_header_flag("Authorization: Bearer x"),
            Ok(("Authorization".into(), "Bearer x".into()))
        );
        assert!(parse_header_flag("no colon").is_err());
    }

    #[test]
    fn parse_status_list_form() {
        let ok = parse_status_list("200,201,204").unwrap();
        assert_eq!(ok.0, vec![200, 201, 204]);
        assert!(parse_status_list("200,xyz").is_err());
    }

    #[test]
    fn args_minimal_url_and_saturate() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-c",
            "10",
            "-d",
            "5s",
            "http://127.0.0.1:1234/",
        ])
        .unwrap();
        assert!(args.saturate);
        assert_eq!(args.connections, 10);
        assert_eq!(args.duration, Duration::from_secs(5));
        assert_eq!(args.url.as_deref(), Some("http://127.0.0.1:1234/"));
    }

    #[test]
    fn args_request_file_implies_no_url_needed() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--request-file",
            "/tmp/foo.http",
            "--saturate",
        ])
        .unwrap();
        assert_eq!(
            args.request_file.as_deref().unwrap().to_str(),
            Some("/tmp/foo.http"),
        );
        assert!(args.url.is_none());
    }

    #[test]
    fn args_requests_dir_mutex_with_request_file() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--request-file",
            "/tmp/a.http",
            "--requests",
            "/tmp/dir",
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("cannot be used"));
    }

    #[test]
    fn args_diff_subcommand_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "diff",
            "/tmp/base.json",
            "/tmp/curr.json",
            "--threshold-p99",
            "3.5",
        ])
        .unwrap();
        match args.command.as_ref().expect("subcommand") {
            Subcommand::Diff(da) => {
                assert_eq!(da.baseline.to_str(), Some("/tmp/base.json"));
                assert_eq!(da.current.to_str(), Some("/tmp/curr.json"));
                assert!((da.threshold_p99 - 3.5).abs() < 1e-9);
            }
            Subcommand::Run(_) => panic!("expected Diff, got Run"),
            Subcommand::Measure(_) => panic!("expected Diff, got Measure"),
            Subcommand::Probe(_) => panic!("expected Diff, got Probe"),
            Subcommand::Compare(_) => panic!("expected Diff, got Compare"),
            Subcommand::Calibrate(_) => panic!("expected Diff, got Calibrate"),
            Subcommand::Curve(_) => panic!("expected Diff, got Curve"),
        }
    }

    #[test]
    fn args_diff_defaults_are_sensible() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "diff",
            "base.json",
            "curr.json",
        ])
        .unwrap();
        match args.command.as_ref().expect("subcommand") {
            Subcommand::Diff(da) => {
                assert!((da.threshold_p99 - 5.0).abs() < 1e-9);
                assert!((da.threshold_rps - 2.0).abs() < 1e-9);
            }
            Subcommand::Run(_) => panic!("expected Diff, got Run"),
            Subcommand::Measure(_) => panic!("expected Diff, got Measure"),
            Subcommand::Probe(_) => panic!("expected Diff, got Probe"),
            Subcommand::Compare(_) => panic!("expected Diff, got Compare"),
            Subcommand::Calibrate(_) => panic!("expected Diff, got Calibrate"),
            Subcommand::Curve(_) => panic!("expected Diff, got Curve"),
        }
    }

    #[test]
    fn args_rate_and_saturate_are_mutually_exclusive() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-r",
            "100",
            "http://127.0.0.1:1/",
        ])
        .unwrap_err();
        // clap's "conflicts_with" error kind.
        assert!(format!("{err}").contains("cannot be used"));
    }

    #[test]
    fn args_repeatable_header() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-H",
            "X-A: 1",
            "-H",
            "X-B: 2",
            "http://127.0.0.1:1/",
        ])
        .unwrap();
        assert_eq!(args.headers.len(), 2);
    }

    #[test]
    fn args_default_duration_is_30s() {
        let args = CliArgs::try_parse_from(["zerobench", "http://127.0.0.1:1/"])
            .unwrap();
        assert_eq!(args.duration, Duration::from_secs(30));
        assert_eq!(args.connections, 50);
    }

    #[test]
    fn args_request_file_conflicts_with_positional_url() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--request-file",
            "/tmp/foo.http",
            "http://example/",
        ])
        .unwrap_err();
        // clap emits a `cannot be used` message for mutually-exclusive args.
        assert!(
            format!("{err}").contains("cannot be used"),
            "expected conflict error, got: {err}"
        );
    }

    #[test]
    fn args_requests_dir_conflicts_with_positional_url() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--requests",
            "/tmp/dir",
            "http://example/",
        ])
        .unwrap_err();
        assert!(
            format!("{err}").contains("cannot be used"),
            "expected conflict error, got: {err}"
        );
    }

    #[test]
    fn parse_threshold_rejects_negative() {
        assert!(parse_threshold_flag("-1").is_err());
        assert!(parse_threshold_flag("-0.01").is_err());
        assert!(parse_threshold_flag("NaN").is_err());
        assert_eq!(parse_threshold_flag("0").unwrap(), 0.0);
        assert_eq!(parse_threshold_flag("5").unwrap(), 5.0);
        assert!((parse_threshold_flag("2.5").unwrap() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn args_diff_rejects_negative_threshold() {
        // Use `=` syntax so clap doesn't interpret `-3` as a short flag.
        let err = CliArgs::try_parse_from([
            "zerobench",
            "diff",
            "b.json",
            "c.json",
            "--threshold-p99=-3",
        ])
        .unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(
            msg.contains("threshold") || msg.contains("invalid"),
            "expected threshold error, got: {msg}"
        );
    }

    #[test]
    fn args_threads_flag_short_form() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "-t",
            "4",
            "-c",
            "20",
            "-d",
            "1s",
            "http://127.0.0.1:1/",
        ])
        .unwrap();
        assert_eq!(args.threads, 4);
        assert_eq!(args.connections, 20);
    }

    #[test]
    fn args_threads_flag_long_form() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "--threads",
            "8",
            "http://127.0.0.1:1/",
        ])
        .unwrap();
        assert_eq!(args.threads, 8);
    }

    #[test]
    fn args_threads_defaults_to_cpu_count() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--saturate",
            "http://127.0.0.1:1/",
        ])
        .unwrap();
        // Default should be num_cpus(), which is >= 1.
        assert!(args.threads >= 1);
        assert_eq!(args.threads, num_cpus());
    }

    // ---------- new flags (S1.6 — S3.28) ----------

    #[test]
    fn args_warmup_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--warmup",
            "5s",
            "http://h:1/",
        ])
        .unwrap();
        assert_eq!(args.warmup, Some(Duration::from_secs(5)));
    }

    #[test]
    fn args_warmup_invalid_form_rejected() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--warmup",
            "abc",
            "http://h:1/",
        ])
        .unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(msg.contains("invalid") || msg.contains("duration"));
    }

    #[test]
    fn args_basic_auth_parses_and_keeps_value() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--basic-auth",
            "alice:hunter2",
            "http://h:1/",
        ])
        .unwrap();
        assert_eq!(args.basic_auth.as_deref(), Some("alice:hunter2"));
        assert!(args.bearer.is_none());
    }

    #[test]
    fn args_basic_auth_requires_colon() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--basic-auth",
            "no-colon-here",
            "http://h:1/",
        ])
        .unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(msg.contains("user:pass") || msg.contains("expected"));
    }

    #[test]
    fn args_bearer_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--bearer",
            "eyJabc",
            "http://h:1/",
        ])
        .unwrap();
        assert_eq!(args.bearer.as_deref(), Some("eyJabc"));
    }

    #[test]
    fn args_basic_auth_conflicts_with_bearer() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--basic-auth",
            "a:b",
            "--bearer",
            "x",
            "http://h:1/",
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("cannot be used"));
    }

    #[test]
    fn args_json_body_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--json",
            "{\"k\":1}",
            "http://h:1/",
        ])
        .unwrap();
        assert_eq!(args.json.as_deref(), Some("{\"k\":1}"));
    }

    #[test]
    fn args_form_body_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--form",
            "k=v&other=thing",
            "http://h:1/",
        ])
        .unwrap();
        assert_eq!(args.form.as_deref(), Some("k=v&other=thing"));
    }

    #[test]
    fn args_json_conflicts_with_body() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--json",
            "{\"a\":1}",
            "--body",
            "raw",
            "http://h:1/",
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("cannot be used"));
    }

    #[test]
    fn args_json_conflicts_with_form() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--json",
            "{\"a\":1}",
            "--form",
            "k=v",
            "http://h:1/",
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("cannot be used"));
    }

    #[test]
    fn args_resolve_parses_single() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--resolve",
            "example.com:443:10.0.0.5",
            "http://example.com/",
        ])
        .unwrap();
        assert_eq!(args.resolve.len(), 1);
        assert_eq!(args.resolve[0].0, "example.com");
        assert_eq!(args.resolve[0].1, 443);
        assert_eq!(args.resolve[0].2, "10.0.0.5");
    }

    #[test]
    fn args_resolve_repeatable() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--resolve",
            "a.example:80:1.1.1.1",
            "--resolve",
            "b.example:443:2.2.2.2",
            "http://a.example/",
        ])
        .unwrap();
        assert_eq!(args.resolve.len(), 2);
    }

    #[test]
    fn args_resolve_rejects_missing_parts() {
        let err = CliArgs::try_parse_from([
            "zerobench",
            "--resolve",
            "bad-value",
            "http://h:1/",
        ])
        .unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(msg.contains("host:port:addr") || msg.contains("invalid"));
    }

    #[test]
    fn args_http2_prior_knowledge_flag() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--http2-prior-knowledge",
            "http://h:1/",
        ])
        .unwrap();
        assert!(args.http2_prior_knowledge);
    }

    #[test]
    fn args_output_file_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "-o",
            "/tmp/report.txt",
            "http://h:1/",
        ])
        .unwrap();
        assert_eq!(
            args.output.as_deref().and_then(|p| p.to_str()),
            Some("/tmp/report.txt"),
        );
    }

    #[test]
    fn args_dry_run_flag_parses() {
        let args = CliArgs::try_parse_from([
            "zerobench",
            "--dry-run",
            "http://h:1/",
        ])
        .unwrap();
        assert!(args.dry_run);
    }

    #[test]
    fn parse_resolve_valid_and_invalid() {
        let ok = parse_resolve_flag("example.com:443:10.0.0.5").unwrap();
        assert_eq!(ok.0, "example.com");
        assert_eq!(ok.1, 443);
        assert_eq!(ok.2, "10.0.0.5");

        assert!(parse_resolve_flag("missing-everything").is_err());
        assert!(parse_resolve_flag("host:addr").is_err());
        assert!(parse_resolve_flag("host:notaport:1.2.3.4").is_err());
        assert!(parse_resolve_flag(":443:1.1.1.1").is_err());
        assert!(parse_resolve_flag("host:443:").is_err());
    }

    #[test]
    fn parse_basic_auth_valid_and_invalid() {
        assert!(parse_basic_auth_flag("a:b").is_ok());
        assert!(parse_basic_auth_flag("alice:hunter:colon").is_ok());
        assert!(parse_basic_auth_flag("").is_err());
        assert!(parse_basic_auth_flag("nocolon").is_err());
    }

    #[test]
    fn long_version_lists_protocols_and_features() {
        let s = long_version();
        assert!(s.starts_with(env!("CARGO_PKG_VERSION")));
        assert!(s.contains("Protocols: http/1, http/2, sse, websocket"));
        assert!(s.contains("Features: default"));
        assert!(s.contains("mio/epoll"));
    }
}
