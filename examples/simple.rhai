// simple.rhai — minimal example: POST to /events at 1k req/s for 10s.
//
// Run with:
//   zerobench run examples/simple.rhai
//
// The zerobench-rhai loader compiles this script once, drops the Rhai
// engine, and hands a pure-Rust Plan to the benchmark runner.

scenario("events", |s| {
    s.step(
        POST("http://localhost:3000/events")
            .json(#{
                id: "{{uuid}}",
                ts: "{{now_ms}}",
            })
            .expect_status(201)
    );
});

rate("1k/s");
duration("10s");
