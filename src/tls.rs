//! TLS configuration for `wss://` URLs.
//!
//! With rustls, the SDK always builds an explicit [`rustls::ClientConfig`]
//! with a chosen crypto provider and never relies on rustls' process-wide
//! default provider, which panics when more than one provider is compiled
//! into the binary. The provider is aws-lc-rs when the `aws-lc-rs` feature is
//! enabled (even if `ring` is too), else ring.

#[cfg(not(any(feature = "__rustls", feature = "native-tls")))]
compile_error!(
    "ws-mixer needs a TLS stack: enable `rustls-platform-verifier` and `ring` (the defaults), \
     another rustls feature with `ring` or `aws-lc-rs`, or `native-tls`"
);

#[cfg(all(
    feature = "__rustls",
    not(any(feature = "ring", feature = "aws-lc-rs"))
))]
compile_error!(
    "ws-mixer's rustls features need a crypto provider: enable `ring` (default) or `aws-lc-rs`"
);

#[cfg(feature = "__rustls")]
use std::sync::Arc;

/// The crypto provider for every rustls config the SDK builds.
#[cfg(any(feature = "rustls-platform-verifier", feature = "rustls-webpki-roots"))]
fn crypto_provider() -> rustls::crypto::CryptoProvider {
    #[cfg(feature = "aws-lc-rs")]
    return rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
    return rustls::crypto::ring::default_provider();
    #[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
    unreachable!("rejected by compile_error above")
}

/// A `ClientConfig` builder on [`crypto_provider`] with rustls' default
/// protocol versions.
#[cfg(any(feature = "rustls-platform-verifier", feature = "rustls-webpki-roots"))]
pub(crate) fn config_builder() -> rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>
{
    rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("built-in crypto providers support the default protocol versions")
}

/// The rustls configuration for `config`. `NativeTls` is not a rustls
/// configuration and is rejected; the dialer handles it separately.
#[cfg(feature = "__rustls")]
pub(crate) fn rustls_config(config: TlsConfig) -> Result<Arc<rustls::ClientConfig>, rustls::Error> {
    match config {
        #[cfg(feature = "rustls-platform-verifier")]
        TlsConfig::PlatformVerifier => {
            use rustls_platform_verifier::BuilderVerifierExt;
            Ok(Arc::new(
                config_builder()
                    .with_platform_verifier()?
                    .with_no_client_auth(),
            ))
        }
        #[cfg(feature = "rustls-webpki-roots")]
        TlsConfig::WebpkiRoots => {
            let roots =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Ok(Arc::new(
                config_builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ))
        }
        #[cfg(feature = "native-tls")]
        TlsConfig::NativeTls => Err(rustls::Error::General(
            "native-tls is not a rustls configuration".into(),
        )),
        TlsConfig::Custom(c) => Ok(c),
    }
}

/// How `wss://` connections are secured. Ignored for `ws://` URLs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TlsConfig {
    /// rustls with the operating system's certificate verifier (macOS
    /// Security.framework, Windows CryptoAPI, Linux system roots). The default.
    #[cfg(feature = "rustls-platform-verifier")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rustls-platform-verifier")))]
    PlatformVerifier,
    /// rustls with the bundled Mozilla root set from `webpki-roots`.
    #[cfg(feature = "rustls-webpki-roots")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rustls-webpki-roots")))]
    WebpkiRoots,
    /// The platform's native TLS stack via `native-tls`.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    NativeTls,
    /// A caller-supplied rustls configuration, used as is.
    #[cfg(feature = "__rustls")]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(
            feature = "rustls-platform-verifier",
            feature = "rustls-webpki-roots",
            feature = "ring",
            feature = "aws-lc-rs"
        )))
    )]
    Custom(Arc<rustls::ClientConfig>),
}

impl TlsConfig {
    /// The configuration used when none is set: `PlatformVerifier` if that
    /// feature is enabled, else `WebpkiRoots`, else `NativeTls`: rustls is
    /// preferred when both stacks are compiled in. `None` when no built-in
    /// option is compiled in; `wss://` then requires `Custom`.
    pub(crate) fn default_for_build() -> Option<Self> {
        #[cfg(feature = "rustls-platform-verifier")]
        return Some(Self::PlatformVerifier);
        #[cfg(all(
            not(feature = "rustls-platform-verifier"),
            feature = "rustls-webpki-roots"
        ))]
        return Some(Self::WebpkiRoots);
        #[cfg(all(
            not(feature = "rustls-platform-verifier"),
            not(feature = "rustls-webpki-roots"),
            feature = "native-tls"
        ))]
        return Some(Self::NativeTls);
        #[allow(unreachable_code)]
        None
    }
}
