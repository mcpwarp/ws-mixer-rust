# ws-mixer-mcpwarp

The mcpwarp layer over [ws-mixer](https://github.com/mcpwarp/ws-mixer-rust): expose a local MCP
server at a public mcpwarp URL. Implements §2-§8 of
[`docs/MCPWARP.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/v0.8.1/docs/MCPWARP.md) in
ws-mixer-spec **v0.8.1**, on ws-mixer 0.2.1: one service per connection, the scope of every SDK
(§6A, multi-service hosts, is informative host guidance).

An agent dials the edge with a `ws_mixer::Client`, sends a `register` envelope for its service
after every `welcome`, and answers each stream the edge opens as one HTTP/1.1 request to `/mcp`.
This crate gives you three ways to do that:

- **`expose()`**: one call, one client, one registered service, every request served (§5).
- **`Tunnel`**: a long-lived controller for hosts with a settings UI. Call `configure` on every
  settings change and show one `TunnelStatus` (§6, §7).
- **Tier 1 modules**: the envelope, codes, identity, the sans-IO registration machine and
  `bridge::serve_stream`, for an agent that manages its own connection and services.

Native only (tokio). The registration machine is a port of
[ws-mixer-js](https://github.com/mcpwarp/ws-mixer-js)'s `src/mcpwarp/session.ts`.

## Status

0.1.1 on crates.io (2026-10-24, on ws-mixer 0.2.1, built against spec v0.8.0; 0.1.0 was tagged,
never published); `main` pins spec v0.8.1, docs-only over v0.8.0. Every SDK-conformance fixture
of the spec's agent-side sets passes; the host-guidance ones are skipped (see
[Conformance](#conformance)).

The quick starts below also use these crates:

```toml
[dependencies]
ws-mixer-mcpwarp = "0.1"
bytes = "1"
http = "1"
http-body-util = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

For unreleased changes on `main`, depend on a checkout next to your project (the form
mcpwarp-desktop uses), `ws-mixer-mcpwarp = { path = "../ws-mixer-rust/ws-mixer-mcpwarp" }`, or
on the git repository, `ws-mixer-mcpwarp = { git = "https://github.com/mcpwarp/ws-mixer-rust" }`.

## Quick start: `expose()`

```rust
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use ws_mixer_mcpwarp::bridge::{RequestBody, handler_fn};
use ws_mixer_mcpwarp::{ExposeConfig, expose};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Every request is a POST /mcp with the whole body buffered. This one echoes it.
    let app = handler_fn(|req: http::Request<RequestBody>| async move {
        let body: Bytes = req.into_body().collect().await?.to_bytes();
        Ok(http::Response::new(Full::new(body)))
    });
    let handle = expose(
        ExposeConfig::builder()
            .token(std::env::var("MCPWARP_TOKEN")?)
            .name("notes")
            .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
            .handler(app)
            .build()?,
    )
    .await?;
    println!("live at {}{}", handle.url().unwrap_or_default(), handle.path());
    tokio::signal::ctrl_c().await?;
    handle.close().await; // unregister, then close the client
    Ok(())
}
```

`expose()` resolves once the service is registered (or refused as disabled). It fails only with a
fatal `McpwarpError`: a non-transient register refusal, `UNSUPPORTED_VERSION`, `UNAUTHORIZED`,
`TAKEN_OVER` or `DISCONNECTED_FATAL`. Dial failures and transient register errors are retried
(`on_retry` sees each retry). Dropping the future before it resolves aborts it (§5.8).

To forward to a local HTTP server instead of a handler, use `.target("http://127.0.0.1:3000/mcp")`
in place of `.handler(app)` (`target` feature, on by default).

`ExposeHandle` gives the URL, server id, `ExposeState` (`watch_state`), the underlying
`ws_mixer::Client` and an `events()` stream of `ExposeEvent`s: `Registered`, `Disabled`,
`Enabled`, `Conflict` (taken over; sticky), `Error`, `HandlerError`, `Disconnect`, `Close`.
Subscribe to `events()` before reading `state()`: events are not replayed.

Runnable: `cargo run -p ws-mixer-mcpwarp --example expose_echo` (see the file header for the
environment variables).

## Quick start: `Tunnel`

```rust
use bytes::Bytes;
use http_body_util::Full;
use ws_mixer_mcpwarp::bridge::handler_fn;
use ws_mixer_mcpwarp::{DEFAULT_DISPOSE_TIMEOUT, Tunnel, TunnelConfig, TunnelOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tunnel = Tunnel::new(
        TunnelOptions::builder()
            .integration("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b")
            .handler(handler_fn(|_req| async {
                Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
            }))
            .build()?,
    )?;
    let _subscription = tunnel.subscribe(|status| println!("{status:?}"));
    tunnel
        .configure(TunnelConfig {
            enabled: true,
            token: Some(std::env::var("MCPWARP_TOKEN")?),
            name: Some("notes".into()),
            ..TunnelConfig::default()
        })
        .await;
    // ... on every settings change: tunnel.configure(new_config).await;
    tokio::signal::ctrl_c().await?;
    tunnel.dispose(DEFAULT_DISPOSE_TIMEOUT)?.await;
    Ok(())
}
```

- `TunnelOptions` (built once) holds what never changes at runtime: integration, agent, TLS,
  body cap, reconnect policy, `on_retry`, and the handler or target. `build()` rejects bad options
  up front with §5.2's messages, except an invalid reconnect policy: that surfaces when a
  connection starts, as state `error` / `INVALID_CONFIG`.
- `TunnelConfig` (`enabled`, `token`, `token_provider`, `name`, `url`, `kind`) is what changes.
  It is `Deserialize`, so it can come straight from the host's settings JSON (`token_provider` is
  skipped); `Debug` never prints the token. Disabled, or a token that trims to empty, is `off`
  without dialing. A config equal to the last one is a no-op, unless a `stop`, a conflict or an
  error came in between. Build literals with `..TunnelConfig::default()`.
- For a token that changes between dials, set `token_provider` instead of `token` (it wins when
  both are set). It is called on every dial, and the same provider `Arc` is the same config
  (`Token` comes from `ws-mixer`, a direct dependency for this):

  ```rust
  use ws_mixer::{Token, TokenError};
  use ws_mixer_mcpwarp::TunnelConfig;

  let provider = Token::provider(|| async {
      std::env::var("MCPWARP_TOKEN").map_err(TokenError::unavailable)
  });
  let config = TunnelConfig {
      enabled: true,
      token_provider: Some(provider.clone()), // keep `provider` to reconfigure without a redial
      name: Some("notes".into()),
      ..TunnelConfig::default()
  };
  ```

  `TokenError::unavailable` is a failed dial, retried with backoff; `TokenError::fatal` is
  `error`/`DISCONNECTED_FATAL`. A token rejected before `welcome` gets one immediate refresh and
  redial (`on_retry` reason `"token-refresh"`, attempt 1, delay 0); a second rejection is
  `error`/`UNAUTHORIZED`.
- `configure` and `stop` are queued when they are called, before the returned future is polled, so
  calls take effect in call order and only the last of a burst dials. Awaiting the future is
  optional.
- `subscribe` calls the listener now and then with every new status, in order, never the same one
  twice in a row; dropping the `Subscription` unsubscribes. `watch_status` is a lossy
  alternative. `status()` returns a copy of the current one.
- `TunnelStatus` serializes as the spec's JSON (`{"state":"online","url":"..."}`; absent fields
  are left out). `describe_status` turns it into localized UI text.
- Dropping the last `Tunnel` clone disposes it in the background.

### In a Tauri app

The shape a desktop host takes: the status goes to the webview on every change, the settings
pane calls one command, and a second command returns the current status as text.

```rust,ignore
use serde::Serialize;
use tauri::{Emitter, Manager, State};
use ws_mixer_mcpwarp::{
    DescribeOptions, StatusText, Subscription, Tunnel, TunnelConfig, TunnelOptions,
    TunnelStatus, describe_status,
};

/// One constant per product, not per install.
const INTEGRATION: &str = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";

struct Host {
    tunnel: Tunnel,
    _subscription: Subscription,
}

#[derive(Clone, Serialize)]
struct StatusView {
    status: TunnelStatus,
    text: StatusText,
}

fn view(status: &TunnelStatus, locale: Option<String>) -> StatusView {
    let options = DescribeOptions { locale, ..DescribeOptions::default() };
    StatusView { status: status.clone(), text: describe_status(status, &options) }
}

#[tauri::command]
async fn configure_tunnel(host: State<'_, Host>, config: TunnelConfig) -> Result<(), String> {
    host.tunnel.configure(config).await;
    Ok(())
}

#[tauri::command]
fn tunnel_status(host: State<'_, Host>, locale: Option<String>) -> StatusView {
    view(&host.tunnel.status(), locale)
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let options = TunnelOptions::builder()
                .integration(INTEGRATION)
                .target("http://127.0.0.1:3000/mcp")
                .build()?;
            // Tunnel::new spawns its task on the current tokio runtime: Tauri's.
            let tunnel = tauri::async_runtime::block_on(async { Tunnel::new(options) })?;
            let app_handle = app.handle().clone();
            let subscription = tunnel.subscribe(move |status| {
                let _ = app_handle.emit("tunnel-status", view(status, None));
            });
            app.manage(Host { tunnel, _subscription: subscription });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![configure_tunnel, tunnel_status])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

Listeners run on the tunnel's task: keep them short (an `emit` is fine). A panicking listener is
caught and logged; the others still run. `cargo run -p ws-mixer-mcpwarp --example tunnel_settings`
is the same flow on stdin/stdout, including the §7 "no token yet" hint on an `off` status.

## Tier 1 modules

For agents that run their own `ws_mixer::Client` (several services on one connection, custom
lifecycle), the building blocks are public on their own:

| Module / items | Spec | What |
|---|---|---|
| `encode_register`, `encode_unregister`, `decode_envelope`, `InboundEnvelope` | §2 | The `app.body.mcpwarp` envelope. Encoding is byte-exact with the fixtures; decoding applies the §2.8 rules (`Ok(None)`: not this layer's message; `Err`: `MALFORMED_ENVELOPE`, non-fatal). |
| `RegisterErrorCode`, `AppErrorCode`, `TRANSIENT_REGISTER_ERRORS`, `McpwarpError`, `ConfigError` | §2.6, §5.2-§5.4 | Error codes with `as_str()` / wire parsing and `is_transient()`; config errors carry §5.2's messages and never echo the value. |
| `is_valid_service_name`, `is_valid_integration_id`, `SERVICE_NAME_RE`, `ServiceKind` | §2.4 | Name and integration-id syntax, without a regex crate. |
| `Identity`, `Agent` | §3.1, §8 | `hello.meta` (`client: "sdk-rust"`), `hello.agent.runtime`/`os`, the `X-Mcpwarp-Integration` and `User-Agent` headers. `Identity::apply` sets them on a `ws_mixer::ClientBuilder` and vetoes the replacement dial for `drain{replaced}`. |
| `session::Session` | §2.6, §2.7, §3.2, §3.3, §5.9 | The sans-IO registration machine for one service: feed it `welcome`, `app`, `drain`, `disconnected`, timer firings; run the `Command`s it queues. One frame in flight, `OVERLOADED` backoff `min(1000 × 2^(n−1), 30000)` ms; a `disable` for an unknown id is held until `registered` brings it, and dropped with its connection; ops delivered after their connection ended are ignored, except `conflict` and `error`. It assumes it is the only controller on its connection: replies are not matched by name (see [Several services on one client](#several-services-on-one-client)). |
| `bridge::serve_stream`, `ServeContext`, `RequestInfo`, `Handler`, `handler_fn`, `Target`, `target_reply` | §4 | One stream as one HTTP/1.1 request: head limits (64 KiB, 10 s), the `/mcp` rule, `503` while unavailable (per connection, or per request with `state_for_request`), the body cap (`413`), byte-exact error replies (head-only for `HEAD`, parse errors included), `400` for what a web `Request` cannot carry (`CONNECT`, `TRACE`, `TRACK`, a NUL in a header value), response framing. Single-service: `X-Mcpwarp-Server-Id` / `-Name` are never used for routing. |
| `describe_status`, `describe_status_raw`, `describe_service_status_raw`, `DescribeOptions`, `STATUS_LOCALES` | §7, §6A.9 | Status as UI text in `en`, `de`, `es`, `fr`, `ja`, `zh-CN`; locale fallback by language, then `en`; per-call overrides. `_raw` takes wire strings, for codes this version does not know. `describe_service_status_raw(state, code, locale)` gives one service's label and detail over wire strings, for a host that shows several services; no overrides. |

### Bridging and `Handler`

`Handler` is implemented for every `tower_service::Service<http::Request<RequestBody>>` that
returns an `http::Response` with a `Bytes` body, so tower middleware composes. `handler_fn` builds
one from an async closure. The request URI is `https://<Host><target>`, hop-by-hop headers are
removed, and the body arrives buffered. A handler error or panic is answered
`500 {"error":"handler error"}` and reported, never propagated.

`Target` (`target` feature) forwards to a local `http://` server. It is a `Handler` and a
`tower_service::Service` itself, so one handler can dispatch on `Host` to several local servers,
each behind its own cached `Target` (cloning shares the connection pool):

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tower_service::Service;
use ws_mixer_mcpwarp::bridge::{BoxError, Handler, Target, TargetBody, handler_fn};

fn dispatcher() -> Result<impl Handler + Clone, BoxError> {
    let timeout = Duration::from_secs(30);
    let targets: Arc<HashMap<&str, Target>> = Arc::new(HashMap::from([
        ("notes.example", Target::try_new("http://127.0.0.1:3001/mcp".parse()?, timeout)?),
        ("files.example", Target::try_new("http://127.0.0.1:3002/mcp".parse()?, timeout)?),
    ]));
    Ok(handler_fn(move |req: http::Request<_>| {
        let targets = targets.clone();
        async move {
            let host = req.uri().host().unwrap_or_default();
            match targets.get(host) {
                Some(target) => target.clone().call(req).await,
                None => Ok(http::Response::builder().status(404).body(TargetBody::empty())?),
            }
        }
    }))
}
```

A `Target` never fails for a slow or missing server: no response headers within the timeout is
`504 {"error":"local server timed out"}`, an unreachable server is
`502 {"error":"local server unreachable (<code>)"}`. Returned unedited, those are written as the
SDK's byte-exact replies; edited by middleware, they are written as ordinary app responses.
`target_reply(&res)` tells them from the local server's own responses:
`Some(TargetReply::Unreachable { code, .. })` or `Some(TargetReply::Timeout)`, also after in-place
edits (a header added, the status changed), `None` once a layer rebuilds the response.

To serve streams from your own client, spawn `serve_stream` per `Event::Stream` (never await it
inline) with a `ServeContext` that reports your registration state:

```rust
use bytes::Bytes;
use http_body_util::Full;
use ws_mixer_mcpwarp::bridge::{RegistrationState, ServeContext, handler_fn, serve_stream};

async fn serve(mut events: ws_mixer::Events) {
    let ctx = ServeContext::new(|| RegistrationState::Registered.into());
    let app = handler_fn(|_req| async {
        Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
    });
    while let Some(event) = events.recv().await {
        if let ws_mixer::Event::Stream(stream) = event {
            tokio::spawn(serve_stream(stream, &ctx, &app));
        }
    }
}
```

### Several services on one client

One `session::Session` per service, all fed the same `welcome`, `drain` and `disconnected`
events. A `Session` created while a connection is already live must be given
`on_welcome(<live conn>)` by you; it is not retroactive. A connection that ended before the
session existed is ignored by its `on_disconnected`.

A `Session` assumes it is the only controller on its connection (one frame in flight, §2.6):
replies are not matched by name, so any `registered`, `unregistered` or whole-frame `error` it is
given answers its outstanding frame, and a `registered` that does not list its service makes it
re-send. Several sessions on one connection are your job to order, and the order is kept on the
triggers, not the frames: a session marks a frame outstanding, records that it sent `register` on
this connection, and starts the 2000 ms close budget for an `unregister` when it emits the
`SendApp`, so hold back its triggers (`on_welcome`, any of its timer firings (transient retry or
`OVERLOADED` re-send), an `enable`, `close()`) until the connection's slot is free and never
buffer an emitted frame. A reply may make the session emit its next queued frame in the same
call, which then holds the slot before any other session may send. Route `registered`,
`unregistered` and a whole-frame `error` only to the session whose frame is on the wire, by frame
order. Free the slot only on a reply, a `Disconnected` or a replacement welcome, never on a
session's local state change, and key the queue by connection id across drains. `disable`,
`enable` and `conflict` carry an id and name and can go to every session (an `enable` as a
held-back trigger).

A `conflict` is per service; a `drain{replaced}` is per connection. Tell them apart by the
`SessionEvent::Conflict` reason (or by `on_drain` returning `true`):

- `"replaced"`: the edge replaced the whole connection. Every session on it enters conflict;
  close the client at once with `Client::close_immediate` and never redial (`Identity::apply`
  already vetoes the replacement dial).
- any other reason (`"taken_over"`): one service lost its name to another install. Keep the
  connection and the other services, ignore that session's `Command::CloseImmediate`, send no
  `unregister` for it (the session already clears that), and never `register` that name again
  until the user reconfigures it.

Streams for that service's `Host` may still arrive until the edge's new holder settles. Per §3.3
they must be reset with `CANCEL` "service taken over by another connection", not answered. The
same goes for a paused service: its `503` should come per `Host`, not for the whole connection.
`ServeContext::state_for_request` gives the state per request, after the head and before any body
byte: return `ServeState::new(..).conflict(true)` for a taken-over `Host` and `serve_stream`
resets the stream; return `Disabled`, `Closing` or `Unavailable` and it answers `503` before the
body is read (§4.4 row 3). A `Host` you do not serve at all stays `Registered`: `503` would claim
the service exists. Its request reaches your handler, which answers `404` (the dispatcher above
does). In the spec's words, "a single-service agent serves every stream with its one service"
(§4.8): the SDK never selects by `Host`, so any routing by `Host` is the handler's.

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use ws_mixer_mcpwarp::bridge::{RegistrationState, ServeContext, ServeState};

/// Each public host's state, keyed by `host_key`, kept current by the event loop from its session.
type States = Arc<RwLock<HashMap<String, ServeState>>>;

/// `Host` is received with case and port kept: lowercase it and drop any `:port`.
fn host_key(host: &str) -> String {
    host.rsplit_once(':').map_or(host, |(name, _port)| name).to_ascii_lowercase()
}

fn context(states: States) -> ServeContext {
    ServeContext::new(|| RegistrationState::Registered.into()).state_for_request(move |req| {
        let states = states.read().unwrap_or_else(|e| e.into_inner());
        // An unknown host goes to the handler, which answers 404.
        states
            .get(&host_key(req.host()))
            .copied()
            .unwrap_or(ServeState::new(RegistrationState::Registered))
    })
}
```

A child server that is restarting is the same case: return `Unavailable` for its `Host`
(`503 {"error":"service unavailable"}`, body unread) and leave the others `Registered`. Answer
from the handler instead only when you need a `Retry-After` header; `target_reply` tells you when
a `Target` found the child down.

## Cargo features

| Feature | Default | Effect |
|---|---|---|
| `target` | yes | `target` mode and `bridge::Target`: forward to a local `http://` server with hyper |
| `rustls-platform-verifier` | yes | ws-mixer's rustls with the OS trust store |
| `ring` | yes | rustls crypto provider: ring |
| `aws-lc-rs` | | rustls crypto provider: aws-lc-rs; wins over `ring` when both are on |
| `rustls-webpki-roots` | | rustls with the bundled Mozilla roots |
| `native-tls` | | the platform TLS library |
| `test-util` | | `test_util::FakeEdge`: a scripted fake edge under `expose()` and `Tunnel` (`TunnelOptionsBuilder::fake_edge`), plus ws-mixer's `test-util` |

The TLS features forward to ws-mixer's features of the same name; see its README for how the
providers coexist. With `default-features = false`, pick a TLS stack: `native-tls`, or a rustls
feature together with `ring` or `aws-lc-rs`. A build without one fails with `compile_error!`.

The crate enables `serde_json/preserve_order`, so `register` / `unregister` keep the spec's key
order byte for byte (ws-mixer's `send_app` takes a `serde_json::Value`). Cargo features are
unified, so every `serde_json::Map` in your build then keeps insertion order instead of sorting
keys. To see whether your graph already has it:
`cargo tree -e features -i serde_json | grep preserve_order`.

MSRV: Rust 1.85 (edition 2024).

## Conformance

The spec's agent-side fixtures (`spec/fixtures/mcpwarp`, MCPWARP.md §10) at v0.8.1. The
multi-service fixtures that §10.6 classifies as host guidance are skipped with a note:

| Set | Result |
|---|---|
| envelope | 68: 52 asserted (encoder byte checks and decodes); 14 agent -> edge invalid cases are edge-runner only and noted; `register_two_services` and `register_batch_valid` skipped (several services in one frame, which this encoder never sends) |
| bridging | 22 of 27 run and pass. Skipped: `body_413_streamed_reset`, which applies only to adapters that stream the request body (this SDK buffers it; `body_413` covers the cap), and the four `multi_service_*` (no multi-service routing) |
| tunnel | 44 of 58: every single-service fixture (01-32 and 34-45) passes on the fake edge with a paused clock; the 14 `ms_*` group fixtures are skipped (no service group) |
| status text | 144 table entries (the multi-service `service.*` and `lastError.NO_SERVICE_REGISTERED` keys included), 37 cases, and the rules file |

Run them with `make mcpwarp-fixtures` from the repository root (it fetches the spec at `spec.pin`
and enables `test-util`, which the tunnel runner needs, and `target`). CI fails a run that skips
anything outside the skips above.

## Deviations and choices

- **Request bodies are buffered**, up to `max_request_body_bytes` (4 MiB by default; larger is
  `413`). Handlers get `http_body_util::Full<Bytes>`.
- **`target` is `http://` only.** `build()` rejects an `https://` target with a `ConfigError`
  (the spec's `INVALID_CONFIG`), so it never becomes a runtime failure.
- **Take-over close.** A `conflict` op or a `drain{replaced}` closes at once with
  `error{NO_ERROR}` and WebSocket close 1000 (`ws_mixer::Client::close_immediate`), skipping the
  §2.10 drain; the streams are stale. The replacement dial for `drain{replaced}` is vetoed before
  it can send `hello`.
- **Status timing.** The tunnel's actor task publishes a status when it processes an input (a
  `configure`, a transport event), not when the call is made. Calls are queued in order, so no
  status is lost or reordered, but `status()` read right after a `configure` call may still show
  the previous one.
- **TLS is a `TunnelOptions` / `ExposeConfig` setting** (`tls(ws_mixer::TlsConfig)`), the Rust
  form of §8's WebSocket factory; it does not change per `configure`.
- **One service per connection.** That is every SDK's scope; since v0.8.0 §6A (one connection
  publishing many services, routed per stream by §4.8) is informative guidance for multi-service
  hosts, which build their own controller. There is no service group and no
  `trust_server_id_header` (§4.8 Trust gate: "An SDK does no routing ... and has no such option"):
  `X-Mcpwarp-Server-Id` and `X-Mcpwarp-Server-Name` reach the application as ordinary headers and
  never select anything. A single-service handle never selects by `Host` either: every `/mcp`
  request that reaches a registered handle goes to its handler, which answers `404` itself if it
  multiplexes internally.
- **A handler status below 200 or from 600 up** is answered `500 {"error":"handler error"}` and
  reported, like a handler error.
- **The 64 KiB head cap counts the blank line** that ends the head (`\r\n\r\n`); a head of
  exactly 64 KiB including it fits.
- In handler and target mode alike, a request the HTTP types cannot represent (a control
  character in a header value, say) or a `CONNECT`, `TRACE` or `TRACK` request is answered
  `400 request cannot be represented as a web Request`, as §4.5 words it.

## License

Apache-2.0. See
[LICENSE](https://github.com/mcpwarp/ws-mixer-rust/blob/main/ws-mixer-mcpwarp/LICENSE) and
[NOTICE](https://github.com/mcpwarp/ws-mixer-rust/blob/main/ws-mixer-mcpwarp/NOTICE).
