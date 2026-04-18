// Multi-protocol Tier-1 fixture.
//
// Exercises HTTP + SSE + WS scenarios in a single script. The CLI test
// interpolates {{env:TEST_HTTP_URL}} etc. at load time; each URL points
// at an in-process stub listener spawned by the test harness.

scenario("http-ping", |s| {
    s.step(
        GET(env("TEST_HTTP_URL"))
            .expect_status(200)
    );
});

scenario("sse-events", |s| {
    s.step(
        SSE(env("TEST_SSE_URL"))
            .expect_chunks(1)
    );
});

scenario("ws-echo", |s| {
    s.step(
        WS(env("TEST_WS_URL"))
            .message("ping")
    );
});

duration(env("TEST_DURATION", "300ms"));
saturate(4);
