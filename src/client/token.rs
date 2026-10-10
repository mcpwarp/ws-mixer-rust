//! Token acquisition for a dial, bounded by the connect timeout.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt;

use crate::token::{Token, TokenError};

/// The token for one dial. A provider is called every time, never cached,
/// and bounded by `timeout`; a provider that does not answer in time is
/// reported as [`TokenError::Unavailable`] (an SDK choice: a hung refresh
/// endpoint is the textbook temporary failure). A provider that panics is an
/// unmarked failure: [`TokenError::Fatal`] (CLIENT-SDK.md "Provider failure").
pub(crate) async fn fetch(token: &Token, timeout: Duration) -> Result<String, TokenError> {
    match token {
        Token::Static(s) => Ok(s.clone()),
        Token::Provider(p) => {
            let call = std::panic::catch_unwind(AssertUnwindSafe(|| p.token()));
            let call = match call {
                Ok(fut) => AssertUnwindSafe(fut).catch_unwind(),
                Err(panic) => return Err(panicked(panic)),
            };
            match tokio::time::timeout(timeout, call).await {
                Ok(Ok(r)) => r,
                Ok(Err(panic)) => Err(panicked(panic)),
                Err(_) => Err(TokenError::unavailable(format!(
                    "token provider did not answer within {}ms",
                    timeout.as_millis()
                ))),
            }
        }
    }
}

fn panicked(panic: Box<dyn Any + Send>) -> TokenError {
    let detail = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string payload");
    TokenError::fatal(format!("token provider panicked: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn static_and_provider() {
        assert_eq!(
            fetch(&Token::from("s"), Duration::from_secs(1))
                .await
                .unwrap(),
            "s"
        );
        let t = Token::provider(|| async { Ok("p".to_owned()) });
        assert_eq!(fetch(&t, Duration::from_secs(1)).await.unwrap(), "p");
        let t = Token::provider(|| async { Err(TokenError::fatal("signed out")) });
        assert!(matches!(
            fetch(&t, Duration::from_secs(1)).await,
            Err(TokenError::Fatal(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn provider_panic_is_fatal() {
        let t = Token::provider(|| async { panic!("boom") });
        let e = fetch(&t, Duration::from_secs(1)).await.unwrap_err();
        assert!(matches!(e, TokenError::Fatal(_)));
        assert_eq!(
            std::error::Error::source(&e).unwrap().to_string(),
            "token provider panicked: boom"
        );
        let t = Token::provider(|| -> std::future::Ready<Result<String, TokenError>> {
            panic!("sync {}", 7)
        });
        let e = fetch(&t, Duration::from_secs(1)).await.unwrap_err();
        assert_eq!(
            std::error::Error::source(&e).unwrap().to_string(),
            "token provider panicked: sync 7"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn provider_timeout_is_unavailable() {
        let t = Token::provider(std::future::pending);
        let start = tokio::time::Instant::now();
        let e = fetch(&t, Duration::from_secs(3)).await.unwrap_err();
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        assert!(matches!(e, TokenError::Unavailable(_)));
        assert_eq!(
            std::error::Error::source(&e).unwrap().to_string(),
            "token provider did not answer within 3000ms"
        );
    }
}
