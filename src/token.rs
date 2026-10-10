//! Bearer tokens: a static string or a provider called on every dial.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A boxed, thread-safe error, used as the source of a [`TokenError`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The bearer token sent in the `Authorization` header and in `hello.token`.
#[derive(Clone)]
pub enum Token {
    /// The same token on every dial. A pre-welcome `4011` or HTTP `401` with a
    /// static token is fatal.
    Static(String),
    /// A provider called on every dial (including the parallel dial a server
    /// drain triggers); its result is never cached. A pre-welcome `4011` or
    /// HTTP `401` gets one immediate refresh before it is treated as fatal.
    Provider(Arc<dyn TokenProvider>),
}

impl Token {
    /// Wraps an async closure as a [`Token::Provider`].
    ///
    /// ```
    /// use ws_mixer::{Token, TokenError};
    ///
    /// # async fn fetch_token(url: &str) -> std::io::Result<String> { unimplemented!() }
    /// let token = Token::provider(|| async {
    ///     // Missing configuration never fixes itself: fatal.
    ///     let url = std::env::var("EDGE_TOKEN_URL").map_err(TokenError::fatal)?;
    ///     // A failed refresh may succeed on the next dial: retry with backoff.
    ///     fetch_token(&url).await.map_err(TokenError::unavailable)
    /// });
    /// ```
    pub fn provider<F, Fut>(f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + Send + 'static,
    {
        Self::Provider(Arc::new(FnProvider(f)))
    }

    pub(crate) fn is_provider(&self) -> bool {
        matches!(self, Self::Provider(_))
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(_) => f.write_str("Token::Static(<redacted>)"),
            Self::Provider(_) => f.write_str("Token::Provider(..)"),
        }
    }
}

impl From<String> for Token {
    fn from(token: String) -> Self {
        Self::Static(token)
    }
}

impl From<&str> for Token {
    fn from(token: &str) -> Self {
        Self::Static(token.to_owned())
    }
}

/// Supplies a fresh bearer token for each dial.
///
/// The call is bounded by [`ReconnectPolicy::connect_timeout`]; a timeout is
/// reported as [`TokenError::Unavailable`].
///
/// [`ReconnectPolicy::connect_timeout`]: crate::ReconnectPolicy::connect_timeout
pub trait TokenProvider: Send + Sync + 'static {
    /// Returns the token to use for the next dial.
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send + '_>>;
}

struct FnProvider<F>(F);

impl<F, Fut> TokenProvider for FnProvider<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, TokenError>> + Send + 'static,
{
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send + '_>> {
        Box::pin((self.0)())
    }
}

/// A [`TokenProvider`] could not produce a token.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TokenError {
    /// The token is temporarily unavailable (e.g. the refresh endpoint is
    /// unreachable). Reported as a non-fatal dial-phase disconnect; the client
    /// backs off normally and the attempt counts toward `max_attempts`. If it
    /// happens on the refresh after a `401`/`4011`, it spends that refresh.
    #[error("token temporarily unavailable")]
    Unavailable(#[source] BoxError),
    /// The provider will never succeed (e.g. the user signed out). Fatal; the
    /// error is surfaced verbatim in [`DisconnectReason::cause`].
    ///
    /// [`DisconnectReason::cause`]: crate::DisconnectReason::cause
    #[error("token provider failed")]
    Fatal(#[source] BoxError),
}

impl TokenError {
    /// Builds a [`TokenError::Unavailable`].
    pub fn unavailable(e: impl Into<BoxError>) -> Self {
        Self::Unavailable(e.into())
    }

    /// Builds a [`TokenError::Fatal`].
    pub fn fatal(e: impl Into<BoxError>) -> Self {
        Self::Fatal(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_impls() {
        assert!(matches!(Token::from("a"), Token::Static(s) if s == "a"));
        assert!(matches!(Token::from(String::from("b")), Token::Static(s) if s == "b"));
        assert!(!Token::from("a").is_provider());
    }

    #[test]
    fn debug_redacts() {
        assert_eq!(
            format!("{:?}", Token::from("secret")),
            "Token::Static(<redacted>)"
        );
    }

    #[tokio::test]
    async fn provider_is_called_each_time() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let token = Token::provider(move || {
            let n = n2.clone();
            async move { Ok(format!("t{}", n.fetch_add(1, Ordering::SeqCst))) }
        });
        let Token::Provider(p) = &token else {
            panic!("expected provider")
        };
        assert!(token.is_provider());
        assert_eq!(p.token().await.unwrap(), "t0");
        assert_eq!(p.token().await.unwrap(), "t1");
    }

    #[test]
    fn error_constructors() {
        let e = TokenError::unavailable("offline");
        assert!(matches!(e, TokenError::Unavailable(_)));
        assert_eq!(e.to_string(), "token temporarily unavailable");
        assert_eq!(
            std::error::Error::source(&e).unwrap().to_string(),
            "offline"
        );
        let e = TokenError::fatal(std::io::Error::other("signed out"));
        assert!(matches!(e, TokenError::Fatal(_)));
        assert!(std::error::Error::source(&e).is_some());
    }
}
