//! The seam between the `expose()` driver and [`ws_mixer::Client`]: what the
//! driver sends ([`AppSender`]), what it receives ([`EventSource`]), and how
//! the pair is made from a configured [`ClientBuilder`] ([`Connector`]). The
//! real client implements it; so does the `test-util` fake edge.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use ws_mixer::{BuildError, Client, ClientBuilder, Event, Events, SendAppError};

pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The outbound half: `app` messages and the two ways of closing.
pub(crate) trait AppSender: Send + Sync + 'static {
    /// [`Client::send_app`].
    fn send_app(&self, body: Value) -> BoxFut<'_, Result<(), SendAppError>>;
    /// [`Client::close`]: `drain{client_requested}`, grace, close 1000.
    fn close(&self) -> BoxFut<'static, ()>;
    /// [`Client::close_immediate`]: `error{NO_ERROR}` + close 1000 at once.
    fn close_immediate(&self, message: String) -> BoxFut<'static, ()>;
}

/// The inbound half: the client's events, `None` once it has finished.
pub(crate) trait EventSource: Send + 'static {
    /// [`Events::recv`]. Cancel-safe.
    fn recv(&mut self) -> BoxFut<'_, Option<Event>>;
}

/// A started client.
pub(crate) struct Transport {
    pub(crate) sender: Arc<dyn AppSender>,
    pub(crate) events: Box<dyn EventSource>,
    /// The real client, for [`ExposeHandle::client`](crate::ExposeHandle::client).
    pub(crate) client: Option<Client>,
}

/// Starts a client from a configured builder.
pub(crate) trait Connector: Send + 'static {
    fn connect(self: Box<Self>, builder: ClientBuilder) -> Result<Transport, BuildError>;
}

/// The real thing: [`ClientBuilder::connect`].
pub(crate) struct ClientConnector;

impl Connector for ClientConnector {
    fn connect(self: Box<Self>, builder: ClientBuilder) -> Result<Transport, BuildError> {
        let (client, events) = builder.connect()?;
        Ok(Transport {
            sender: Arc::new(client.clone()),
            events: Box::new(events),
            client: Some(client),
        })
    }
}

impl AppSender for Client {
    fn send_app(&self, body: Value) -> BoxFut<'_, Result<(), SendAppError>> {
        Box::pin(Client::send_app(self, body))
    }

    fn close(&self) -> BoxFut<'static, ()> {
        let client = self.clone();
        Box::pin(async move { client.close().await })
    }

    fn close_immediate(&self, message: String) -> BoxFut<'static, ()> {
        let client = self.clone();
        Box::pin(async move { client.close_immediate(message).await })
    }
}

impl EventSource for Events {
    fn recv(&mut self) -> BoxFut<'_, Option<Event>> {
        Box::pin(Events::recv(self))
    }
}
