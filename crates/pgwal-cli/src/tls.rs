use std::io;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

/// The `SSLRequest` code of the Postgres protocol.
const SSL_REQUEST: i32 = 80877103;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    /// No TLS.
    Disable,
    /// TLS, without a check of the server certificate.
    Require,
    /// TLS, with a check of the certificate chain and of the host name.
    VerifyFull,
    /// Values of libpq that pgwal refuses: it never falls back to clear
    /// text, and checks the host name with the chain. They parse, so that a
    /// PGSSLMODE set for psql gives a clear error.
    #[value(hide = true)]
    Allow,
    #[value(hide = true)]
    Prefer,
    #[value(hide = true)]
    VerifyCa,
}

/// The TLS connector for `mode`, or `None` for `disable`. `root_cert` is
/// a PEM file of trusted CAs; without it, `verify-full` trusts the Mozilla
/// roots. Without a mode, a root file gives `verify-full`, as
/// `sslrootcert=system` does in libpq.
pub fn connector(mode: Option<SslMode>, root_cert: Option<&Path>) -> Result<Option<TlsConnector>> {
    let mode = match (mode, root_cert) {
        (None, None) => SslMode::Disable,
        (None, Some(_)) => SslMode::VerifyFull,
        // A root file asks for a check of the certificate, which disable
        // and require do not make.
        (Some(SslMode::Disable | SslMode::Require), Some(_)) => {
            bail!("--sslrootcert needs --sslmode verify-full")
        }
        (Some(mode), _) => mode,
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = || {
        ClientConfig::builder_with_provider(provider.clone()).with_safe_default_protocol_versions()
    };
    let mut config = match mode {
        SslMode::Disable => return Ok(None),
        SslMode::Allow | SslMode::Prefer | SslMode::VerifyCa => {
            let name = mode
                .to_possible_value()
                .map(|value| value.get_name().to_owned())
                .unwrap_or_default();
            bail!("sslmode {name} is not supported: give disable, require or verify-full")
        }
        SslMode::Require => builder()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyCert(provider.clone())))
            .with_no_client_auth(),
        SslMode::VerifyFull => {
            let mut roots = RootCertStore::empty();
            // libpq takes `system` for the trusted roots of the host. pgwal
            // has the Mozilla roots for that.
            match root_cert.filter(|path| *path != Path::new("system")) {
                Some(path) => {
                    for cert in CertificateDer::pem_file_iter(path)
                        .with_context(|| format!("reading {}", path.display()))?
                    {
                        roots.add(cert?)?;
                    }
                }
                None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
            }
            builder()?
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
    };
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(Some(TlsConnector::from(Arc::new(config))))
}

/// Asks the server behind `stream` for TLS and runs the handshake. A
/// server that refuses TLS gives a [`TlsRefused`] error.
pub async fn connect(
    mut stream: TcpStream,
    connector: &TlsConnector,
    host: &str,
) -> io::Result<TlsStream<TcpStream>> {
    let mut request = [0u8; 8];
    request[..4].copy_from_slice(&8i32.to_be_bytes());
    request[4..].copy_from_slice(&SSL_REQUEST.to_be_bytes());
    stream.write_all(&request).await?;
    // One byte only: bytes after it would come from outside the TLS session.
    match stream.read_u8().await? {
        b'S' => {}
        b'N' => {
            return Err(io::Error::other(TlsRefused(format!(
                "the server {host} refuses TLS"
            ))));
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected answer {other:#04x} to SSLRequest"),
            ));
        }
    }
    let name = ServerName::try_from(host.to_owned())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    connector.connect(name, stream).await.map_err(|e| {
        let Some(rustls::Error::InvalidCertificate(cert)) = e
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        else {
            return e;
        };
        // A renewal or a clock fixes these: the next retry can pass.
        if matches!(
            cert,
            CertificateError::Expired
                | CertificateError::ExpiredContext { .. }
                | CertificateError::NotValidYet
                | CertificateError::NotValidYetContext { .. }
        ) {
            return e;
        }
        let hint = match cert {
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. } => {
                " It needs the host name in its subjectAltName."
            }
            CertificateError::UnknownIssuer => " --sslrootcert gives the CA that signed it.",
            // rustls passes this webpki error on as text only.
            CertificateError::Other(other) if other.to_string() == "CaUsedAsEndEntity" => {
                " It is a CA certificate, as a self-signed certificate is: the server needs a \
                 certificate that a CA signed, and --sslrootcert gives that CA."
            }
            _ => "",
        };
        io::Error::other(TlsRefused(format!(
            "the certificate of {host} fails the check: {e}.{hint}"
        )))
    })
}

/// A TLS failure that every retry meets: a server that refuses TLS, or a
/// certificate that fails the check for good.
#[derive(Debug)]
pub struct TlsRefused(String);

impl std::fmt::Display for TlsRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsRefused {}

/// Accepts any server certificate, but checks the handshake signatures.
#[derive(Debug)]
struct AnyCert(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_root_file_asks_for_verify_full() {
        let root = Path::new("system");
        assert!(connector(None, None).unwrap().is_none());
        assert!(connector(None, Some(root)).unwrap().is_some());
        assert!(connector(Some(SslMode::Require), Some(root)).is_err());
        assert!(connector(Some(SslMode::Disable), Some(root)).is_err());
        assert!(connector(Some(SslMode::Require), None).unwrap().is_some());
        assert!(connector(Some(SslMode::Prefer), None).is_err());
    }

    #[tokio::test]
    async fn a_self_signed_server_certificate_gets_a_hint() {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        use rustls::pki_types::PrivateKeyDer;

        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let ca = tmp.path().join("ca.pem");
        std::fs::write(&ca, cert.pem()).unwrap();

        let server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            PrivateKeyDer::try_from(key.serialize_der()).unwrap(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.read_exact(&mut [0; 8]).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
            let _ = acceptor.accept(stream).await;
        });

        let connector = connector(Some(SslMode::VerifyFull), Some(&ca))
            .unwrap()
            .unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let err = connect(stream, &connector, "127.0.0.1").await.unwrap_err();
        assert!(err.to_string().contains("a self-signed certificate"), "{err}");
    }
}
