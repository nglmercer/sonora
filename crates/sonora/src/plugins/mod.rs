pub mod rest;
pub mod websocket;

use std::sync::Arc;

use gpui::App;
use state::{AppSettings, Registration};

/// Attaches the plugin manager with the built-in transports.
pub fn attach(cx: &mut App) {
    state::attach_plugins(
        cx,
        vec![
            Registration {
                plugin: Arc::new(rest::RestPlugin),
                enabled: AppSettings::rest_api,
                port: AppSettings::rest_port,
            },
            Registration {
                plugin: Arc::new(websocket::WsPlugin),
                enabled: AppSettings::ws_api,
                port: AppSettings::ws_port,
            },
        ],
    );
}
