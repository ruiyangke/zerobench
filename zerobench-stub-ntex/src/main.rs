//! `zerobench-stub-ntex` — ntex + compio stub server.
//!
//! Same endpoints as `zerobench-stub` (raw httparse version) but uses the
//! ntex web framework with compio/io_uring backend. Head-to-head benchmark
//! to compare framework overhead vs raw TCP + httparse.

use std::time::Duration;

use ntex::http::StatusCode;
use ntex::util::Bytes;
use ntex::web::{self, App, HttpRequest, HttpResponse};

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn pong() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain")
        .body("pong")
}

async fn health() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(r#"{"status":"ok"}"#)
}

async fn echo(body: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/octet-stream")
        .body(body)
}

async fn delay(path: web::types::Path<u64>) -> HttpResponse {
    let ms = path.into_inner();
    if ms > 0 {
        ntex::time::sleep(Duration::from_millis(ms)).await;
    }
    HttpResponse::Ok()
        .content_type("text/plain")
        .body("ok")
}

async fn status(path: web::types::Path<u16>) -> HttpResponse {
    let code = path.into_inner();
    HttpResponse::build(StatusCode::from_u16(code).unwrap_or(StatusCode::OK))
        .body("")
}

async fn gen_bytes(path: web::types::Path<usize>) -> HttpResponse {
    let n = path.into_inner().min(10_000_000);
    let data = vec![0x42u8; n];
    HttpResponse::Ok()
        .content_type("application/octet-stream")
        .body(data)
}

async fn sse(req: HttpRequest) -> HttpResponse {
    let query = req.uri().query().unwrap_or("");
    let chunks: usize = parse_param(query, "chunks", 10);
    let delay_ms: u64 = parse_param(query, "delay_ms", 100);
    let size: usize = parse_param(query, "size", 50);

    let padding = "x".repeat(size);

    let (tx, rx) = ntex::channel::mpsc::channel::<Result<Bytes, std::io::Error>>();

    ntex::rt::spawn(async move {
        for i in 0..chunks {
            if delay_ms > 0 {
                ntex::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let payload = format!("data: chunk-{i}-{padding}\n\n");
            if tx.send(Ok(Bytes::from(payload))).is_err() {
                return;
            }
        }
        let _ = tx.send(Ok(Bytes::from_static(b"data: [DONE]\n\n")));
    });

    HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(rx)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[ntex::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut port: u16 = 8080;
    let mut workers: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-p" | "--port" => {
                i += 1;
                port = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(8080);
            }
            "-w" | "--workers" => {
                i += 1;
                workers = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(1);
            }
            "-h" | "--help" => {
                eprintln!(
                    "zerobench-stub-ntex [OPTIONS]\n\n\
                     Options:\n  \
                       -p, --port <PORT>     Listen port [default: 8080]\n  \
                       -w, --workers <N>     Worker threads [default: num_cpus]\n  \
                       -h, --help            Show this help"
                );
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    eprintln!("[ntex-stub] {workers} workers on port {port}");
    eprintln!("[ntex-stub] routes: GET / /health /delay/{{ms}} /status/{{code}} /bytes/{{n}} /sse  POST /echo");
    eprintln!("[ntex-stub] http://0.0.0.0:{port}");

    web::server(async || {
        App::new()
            .service(web::resource("/").route(web::get().to(pong)))
            .service(web::resource("/health").route(web::get().to(health)))
            .service(web::resource("/echo").route(web::post().to(echo)))
            .service(web::resource("/delay/{ms}").route(web::get().to(delay)))
            .service(web::resource("/status/{code}").route(web::get().to(status)))
            .service(web::resource("/bytes/{n}").route(web::get().to(gen_bytes)))
            .service(web::resource("/sse").route(web::get().to(sse)))
    })
    .workers(workers)
    .bind(format!("0.0.0.0:{port}"))?
    .run()
    .await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_param<T: std::str::FromStr>(query: &str, key: &str, default: T) -> T {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return v.parse().unwrap_or(default);
            }
        }
    }
    default
}
