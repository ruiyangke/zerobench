//! End-to-end tests for `zerobench diff`.

use std::process::Command;

fn zerobench_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zerobench")
}

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

#[test]
fn diff_near_identical_runs_exits_zero() {
    let out = Command::new(zerobench_bin())
        .args([
            "diff",
            fixture("bench-baseline.json").to_str().unwrap(),
            fixture("bench-current.json").to_str().unwrap(),
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench diff");

    assert!(
        out.status.success(),
        "expected diff to exit 0, got {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("OK"), "expected OK marker, got:\n{stdout}");
}

#[test]
fn diff_p99_regression_exits_one_and_mentions_p99() {
    let out = Command::new(zerobench_bin())
        .args([
            "diff",
            fixture("bench-baseline.json").to_str().unwrap(),
            fixture("bench-regressed.json").to_str().unwrap(),
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench diff");

    assert!(
        !out.status.success(),
        "expected diff to exit 1 (regression), got {:?}\nstdout:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
    );
    let code = out.status.code().unwrap_or(-1);
    assert_eq!(code, 1, "expected exit code 1, got {code}");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("p99"),
        "expected output to mention p99, got:\n{stdout}"
    );
    assert!(
        stdout.contains("REGRESSION"),
        "expected REGRESSION marker, got:\n{stdout}"
    );
}

#[test]
fn diff_threshold_override_suppresses_regression() {
    // bench-regressed has p99 +15% — with --threshold-p99 20, that
    // should fall below the bar and the diff should exit 0.
    let out = Command::new(zerobench_bin())
        .args([
            "diff",
            fixture("bench-baseline.json").to_str().unwrap(),
            fixture("bench-regressed.json").to_str().unwrap(),
            "--threshold-p99",
            "20",
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench diff");

    // NOTE: bench-regressed also has error count going up (5xx: 0→3)
    // and rps dropping by 1.6% (below 2% default). With the default
    // rps threshold we'd still pass on rps, but the error count still
    // triggers a regression. To isolate the p99 threshold test, we
    // verify that — with a high p99 threshold — the diff output at
    // least correctly shows p99 as OK, even if the overall exit is
    // non-zero due to the 5xx count.
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Find the p99 row — it shouldn't show REGRESSION.
    let p99_line = stdout.lines().find(|l| l.starts_with("p99 ")).unwrap_or("");
    assert!(
        !p99_line.contains("REGRESSION"),
        "p99 should not be a regression at threshold=20, got line: {p99_line:?}\nfull:\n{stdout}"
    );
}

#[test]
fn diff_json_format_is_valid_and_has_regression_flag() {
    let out = Command::new(zerobench_bin())
        .args([
            "diff",
            fixture("bench-baseline.json").to_str().unwrap(),
            fixture("bench-regressed.json").to_str().unwrap(),
            "--format",
            "json",
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench diff");

    let stdout = std::str::from_utf8(&out.stdout).expect("utf-8");
    let v: serde_json::Value = serde_json::from_str(stdout).expect("parse json");
    assert!(v.get("regression").is_some());
    assert_eq!(v["regression"], serde_json::Value::from(true));
    assert!(v.get("deltas").is_some());
    // p99 entry should be in the deltas.
    assert!(
        v["deltas"].get("p99_ns").is_some(),
        "deltas: {:?}",
        v["deltas"]
    );
}

#[test]
fn diff_json_on_matching_fixtures_flags_no_regression() {
    let out = Command::new(zerobench_bin())
        .args([
            "diff",
            fixture("bench-baseline.json").to_str().unwrap(),
            fixture("bench-current.json").to_str().unwrap(),
            "--format",
            "json",
            "--color",
            "never",
        ])
        .output()
        .expect("run zerobench diff");

    assert!(out.status.success());
    let stdout = std::str::from_utf8(&out.stdout).expect("utf-8");
    let v: serde_json::Value = serde_json::from_str(stdout).expect("parse json");
    assert_eq!(v["regression"], serde_json::Value::from(false));
}
