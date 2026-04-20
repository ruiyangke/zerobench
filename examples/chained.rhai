// chained.rhai — login once, extract the bearer token, then do 3
// authenticated requests per iteration. Saturate 20 concurrent workers
// for 30 seconds.
//
// Run with:
//   USER_EMAIL=demo@example.com USER_PASSWORD=changeme \
//     zerobench run examples/chained.rhai

// Declare the slot up-front so both the extractor (login step) and the
// templates ({{var:token}}) resolve to the same registry entry. The
// function is called `slot` (not `var`) because `var` is a reserved
// keyword in Rhai.
let token = slot("token");

scenario("user-session", |s| {
    s.step(
        POST("https://api.example.com/login")
            .json(#{
                email:    env("USER_EMAIL", "demo@example.com"),
                password: env("USER_PASSWORD", "changeme"),
            })
            .expect_status(200)
            .extract_header("X-Auth-Token", token)
    );

    s.step(
        GET("https://api.example.com/me")
            .header("Authorization", "Bearer {{var:token}}")
            .expect_status(200)
    );

    // pause()/pause_random() are not currently executed by any
    // backend — they were removed from the DSL rather than kept as
    // silent no-ops. If you need inter-request think time, use
    // separate scenarios with explicit rate("10/s") pacing, or wait
    // for multi-step + per-connection ready-at support.

    s.step(
        GET("https://api.example.com/api/feed")
            .header("Authorization", "Bearer {{var:token}}")
            .expect_status(200)
    );
});

duration("30s");
saturate(20);
