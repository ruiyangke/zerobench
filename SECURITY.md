# Security Policy

## Supported versions

Only the latest released version of `zerobench` (and its workspace
crates) receives security fixes. Security backports to older versions
are not provided.

| Version | Supported |
| ------- | --------- |
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Reporting a vulnerability

**Do not open a public GitHub issue for security-sensitive reports.**

Please report suspected vulnerabilities privately, either via:

- GitHub's private vulnerability reporting
  (**Security → Advisories → Report a vulnerability** on this
  repository), or
- Email to the maintainer listed in `Cargo.toml` (`authors` field).

Include a minimal reproduction (or enough detail to construct one),
the affected version, and the observed impact. A fix or advisory will
typically follow within two weeks.

## Scope

`zerobench` is an outbound HTTP/SSE/WS client that a user runs
deliberately against a target of their choice. It does not listen on
the network (the bundled `zerobench-stub` is test-only and bound to
loopback). The relevant threat model is therefore narrow:

- **Malicious target server** — responses parsed by `zerobench` could
  try to exploit the response-side parser (HTTP/1 via `httparse`,
  HTTP/2 via `h2`, SSE via the WHATWG line framer, WebSocket via the
  RFC 6455 codec). Reports of parser DoS or crash bugs are in scope.
- **Malicious Rhai script** — a `.rhai` scenario a user runs can read
  arbitrary files (`body_file`), read env vars, and fire HTTP
  requests. This is *intentional* (the DSL is a programmable
  front-end); do not run untrusted scripts. Sandboxing is not a
  supported feature.
- **Archive layout** — run artefacts are written under
  `$ZEROBENCH_HOME` (or `$HOME/.zerobench`) with file names derived
  from SHA-256 fingerprints. Path traversal via user input is not a
  surface because those components are hash output, not free-form.

Out of scope: the `zerobench-stub` test server, the TLS `--insecure`
flag (documented as `curl -k`-equivalent), and any issue reachable
only by running a script the user themselves wrote.
