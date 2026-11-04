use std::io;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
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
}

/// The TLS connector for `mode`, or `None` for `disable`. `root_cert` is
/// a PEM file of trusted CAs; without it, `verify-full` trusts the Mozilla
/// roots.
pub fn connector(mode: SslMode, root_cert: Option<&Path>) -> Result<Option<TlsConnector>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let mut config = match mode {
        SslMode::Disable => return Ok(None),
        SslMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyCert(provider)))
            .with_no_client_auth(),
        SslMode::VerifyFull => {
            let mut roots = RootCertStore::empty();
            match root_cert {
                Some(path) => {
                    for cert in CertificateDer::pem_file_iter(path)
                        .with_context(|| format!("reading {}", path.display()))?
                    {
                        roots.add(cert?)?;
                    }
                }
                None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
    };
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(Some(TlsConnector::from(Arc::new(config))))
}

/// Asks the server behind `stream` for TLS and runs the handshake. A server
/// that refuses TLS gives an error of kind `Unsupported`.
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
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the server refuses TLS",
            ));
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
    connector.connect(name, stream).await
}

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
