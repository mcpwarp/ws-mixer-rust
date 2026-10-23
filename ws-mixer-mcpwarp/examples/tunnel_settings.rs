//! A settings pane driving a [`Tunnel`]: each line on stdin is the host's
//! settings as JSON, applied with `configure`; the status is printed as UI
//! text in the settings' locale. An empty line or end of input disposes the
//! tunnel and exits.
//!
//! ```sh
//! MCPWARP_INTEGRATION=6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b \
//! cargo run -p ws-mixer-mcpwarp --example tunnel_settings
//! {"enabled": true, "token": "mcpwarp_pat_...", "name": "notes", "locale": "de"}
//! {"enabled": false}
//! ```
//!
//! An unset token shows the host's own hint on the `off` status (MCPWARP.md
//! §7), not an error.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::Full;
use serde::Deserialize;
use ws_mixer_mcpwarp::bridge::handler_fn;
use ws_mixer_mcpwarp::{
    DEFAULT_DISPOSE_TIMEOUT, DescribeOptions, Tunnel, TunnelConfig, TunnelOptions, TunnelState,
    TunnelStatus, describe_status,
};

/// What the host stores; every field may be missing.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Settings {
    enabled: bool,
    token: String,
    name: String,
    url: Option<String>,
    locale: Option<String>,
}

impl Settings {
    fn tunnel_config(&self) -> TunnelConfig {
        TunnelConfig {
            enabled: self.enabled,
            token: Some(self.token.clone()),
            name: Some(self.name.clone()),
            url: self.url.clone(),
            ..TunnelConfig::default()
        }
    }
}

/// What the pane shows: the tunnel's status, with the host's hint when
/// there is no token yet.
fn show(status: &TunnelStatus, settings: &Settings) {
    let mut status = status.clone();
    if status.state == TunnelState::Off && settings.enabled && settings.token.trim().is_empty() {
        status.last_error = Some("Add an mcpwarp token in the settings to go online.".into());
    }
    let options = DescribeOptions {
        locale: settings.locale.clone(),
        ..DescribeOptions::default()
    };
    let text = describe_status(&status, &options);
    match (text.detail, status.url) {
        (Some(detail), _) => println!("[{}] {detail}", text.label),
        (None, Some(url)) => println!("[{}] {url}", text.label),
        (None, None) => println!("[{}]", text.label),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let integration =
        std::env::var("MCPWARP_INTEGRATION").map_err(|_| "set MCPWARP_INTEGRATION")?;
    let options = TunnelOptions::builder()
        .integration(integration)
        .handler(handler_fn(|_req| async {
            Ok(http::Response::new(Full::new(Bytes::from_static(b"{}"))))
        }))
        .build()?;
    let tunnel = Tunnel::new(options)?;

    let settings = Arc::new(Mutex::new(Settings::default()));
    let current = settings.clone();
    let _subscription = tunnel.subscribe(move |status| show(status, &current.lock().unwrap()));

    // `configure` queues the change when it is called, so a plain thread can
    // apply settings; awaiting the result is optional.
    let pane = tunnel.clone();
    tokio::task::spawn_blocking(move || {
        for line in std::io::stdin().lines() {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                break;
            }
            match serde_json::from_str::<Settings>(&line) {
                Ok(next) => {
                    let config = next.tunnel_config();
                    *settings.lock().unwrap() = next;
                    drop(pane.configure(config));
                }
                Err(e) => eprintln!("not settings JSON: {e}"),
            }
        }
    })
    .await?;

    tunnel.dispose(DEFAULT_DISPOSE_TIMEOUT)?.await;
    Ok(())
}
