//! A [`WalSink`] in an S3 bucket, over HTTP or HTTPS, with SigV4.
//!
//! - Every request signs the SHA-256 of its body, so the server refuses a
//!   body that changed on the way.
//! - Every PUT carries `If-None-Match: *`, so a store that honors it, such
//!   as AWS, R2 or MinIO, never overwrites an object. A key names one LSN range of one timeline of one system, so the same
//!   key must hold the same bytes. On a 412, the client reads the object
//!   back and fails unless the bytes are equal.
//! - Paths are in the path style: `/bucket/key`.

mod retry;
mod sigv4;
mod xml;

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, Full};
use hyper::header::HeaderValue;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::sync::Semaphore;

use sigv4::Slash;

use crate::WalSink;

/// Requests to the store at one time.
const MAX_IN_FLIGHT: usize = 64;
/// Deletes at one time, within `MAX_IN_FLIGHT`.
const DELETES_IN_FLIGHT: usize = 16;
/// Largest object read: the receiver writes none larger.
const MAX_OBJECT: usize = crate::upload::MAX_OBJECT as usize;
/// Largest list page or error body read. The store is not trusted.
const MAX_SMALL: usize = 16 << 20;

#[derive(Clone)]
pub struct S3Config {
    /// `http://host[:port]` or `https://host[:port]`, with no path.
    pub endpoint: String,
    pub bucket: String,
    /// Put before every key. Empty, or ends with `/`.
    pub prefix: String,
    /// R2 takes `auto`.
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// PEM certificates to trust next to the public roots, for a store
    /// with a private CA.
    pub ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("ca_pem", &self.ca_pem.as_ref().map(Vec::len))
            .finish()
    }
}

type Connector = hyper_rustls::HttpsConnector<HttpConnector>;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

/// HTTP and HTTPS, with the public roots and the extra CA certificates.
fn connector(ca_pem: Option<&[u8]>) -> io::Result<Connector> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(pem) = ca_pem {
        let mut added = 0;
        for cert in CertificateDer::pem_slice_iter(pem) {
            let cert =
                cert.map_err(|e| invalid(format!("the CA certificates do not parse: {e}")))?;
            roots
                .add(cert)
                .map_err(|e| invalid(format!("a CA certificate is not valid: {e}")))?;
            added += 1;
        }
        // A CA file with no certificate is a mistake, not a reason to trust less.
        if added == 0 {
            return Err(invalid("the CA file holds no PEM certificate"));
        }
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| invalid(format!("TLS setup: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .build())
}

/// The error and every cause under it: hyper says "client error (Connect)"
/// and hides the reason.
fn with_causes(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text = format!("{text}: {next}");
        cause = next.source();
    }
    text
}

/// A response with its whole body.
struct Reply {
    status: StatusCode,
    body: Bytes,
    /// Method and URL, so an error names the store and the key.
    target: String,
}

impl Reply {
    fn error(&self) -> io::Error {
        let (code, message) = xml::error(&self.body);
        io::Error::other(format!(
            "{} answered {} {code}: {message}",
            self.target, self.status
        ))
    }
}

/// What one request differs in.
struct Call<'a> {
    method: Method,
    /// The key under the prefix. `None` addresses the bucket.
    key: Option<&'a str>,
    /// Name and value pairs, not encoded.
    query: Vec<(&'static str, &'a str)>,
    headers: Vec<(&'static str, &'static str)>,
    body: Bytes,
    /// The longest reply body this call reads.
    max_reply: usize,
}

impl<'a> Call<'a> {
    fn new(method: Method, key: Option<&'a str>) -> Self {
        Call {
            method,
            key,
            query: Vec::new(),
            headers: Vec::new(),
            body: Bytes::new(),
            max_reply: MAX_SMALL,
        }
    }
}

/// The parts of a call that stay the same from one attempt to the next.
struct Target {
    path: String,
    query: String,
    payload_hash: String,
    uri: String,
}

pub struct S3Sink {
    client: Client<Connector, Full<Bytes>>,
    in_flight: Semaphore,
    /// `http` or `https`.
    scheme: String,
    /// `host[:port]`, for the `host` header.
    authority: String,
    config: S3Config,
}

impl S3Sink {
    /// Sends no request.
    pub fn new(config: S3Config) -> io::Result<S3Sink> {
        let endpoint: Uri = config
            .endpoint
            .parse()
            .map_err(|e| invalid(format!("the endpoint {}: {e}", config.endpoint)))?;
        let (Some(scheme @ ("http" | "https")), Some(authority), "" | "/", None) = (
            endpoint.scheme_str(),
            endpoint.authority(),
            endpoint.path(),
            endpoint.query(),
        ) else {
            return Err(invalid(
                "the endpoint must be http:// or https://, then host[:port], with no path",
            ));
        };
        if !config.prefix.is_empty() && !config.prefix.ends_with('/') {
            return Err(invalid("the key prefix must be empty or end with '/'"));
        }
        if config.bucket.is_empty() {
            return Err(invalid("the bucket name is empty"));
        }
        if config.access_key.trim().is_empty() || config.secret_key.trim().is_empty() {
            return Err(invalid(
                "the access key and the secret key must not be empty",
            ));
        }
        // Both go into the authorization header.
        if HeaderValue::from_str(&format!("{}/{}", config.access_key, config.region)).is_err() {
            return Err(invalid(
                "the access key or the region has a character that a header cannot carry",
            ));
        }
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(MAX_IN_FLIGHT / 2)
            .build(connector(config.ca_pem.as_deref())?);
        Ok(S3Sink {
            client,
            in_flight: Semaphore::new(MAX_IN_FLIGHT),
            scheme: scheme.to_string(),
            authority: authority.to_string(),
            config,
        })
    }

    fn path(&self, key: Option<&str>) -> String {
        let bucket = sigv4::uri_encode(self.config.bucket.as_bytes(), Slash::Encode);
        let Some(key) = key else {
            return format!("/{bucket}");
        };
        let full = format!("{}{key}", self.config.prefix);
        format!(
            "/{bucket}/{}",
            sigv4::uri_encode(full.as_bytes(), Slash::Keep)
        )
    }

    fn target(&self, call: &Call<'_>) -> Target {
        let path = self.path(call.key);
        let query = sigv4::canonical_query(&call.query);
        let mut uri = format!("{}://{}{path}", self.scheme, self.authority);
        if !query.is_empty() {
            uri = format!("{uri}?{query}");
        }
        Target {
            path,
            query,
            payload_hash: sigv4::sha256_hex(&call.body),
            uri,
        }
    }

    fn signed(&self, call: &Call<'_>, target: &Target) -> Request<Full<Bytes>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_secs();
        let timestamp = sigv4::amz_date(now);
        let mut headers = vec![
            ("host".to_string(), self.authority.clone()),
            (
                "x-amz-content-sha256".to_string(),
                target.payload_hash.clone(),
            ),
            ("x-amz-date".to_string(), timestamp.clone()),
        ];
        headers.extend(
            call.headers
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string())),
        );
        let authorization = sigv4::sign(
            &sigv4::Request {
                method: call.method.as_str(),
                path: &target.path,
                query: &target.query,
                headers: &headers,
                payload_hash: &target.payload_hash,
            },
            &self.config.access_key,
            &self.config.secret_key,
            &timestamp,
            &self.config.region,
        );
        let mut request = Request::builder()
            .method(call.method.clone())
            .uri(&target.uri);
        for (name, value) in &headers {
            request = request.header(name.as_str(), value.as_str());
        }
        // hyper sends no length for an empty body, and R2 refuses such a PUT
        // with 411.
        request
            .header("authorization", authorization)
            .header("content-length", call.body.len())
            .body(Full::new(call.body.clone()))
            .expect("a signed request is valid")
    }

    /// Sends a call, signed again for every attempt. A reply with a status
    /// that a retry does not fix comes back as `Ok`.
    async fn send(&self, call: Call<'_>) -> io::Result<Reply> {
        let target = self.target(&call);
        let name = format!("{} {}", call.method, target.uri);
        retry::run(&self.in_flight, || async {
            let response = self
                .client
                .request(self.signed(&call, &target))
                .await
                .map_err(|e| io::Error::other(format!("{name}: {}", with_causes(&e))))?;
            let status = response.status();
            let max = if status.is_success() {
                call.max_reply
            } else {
                MAX_SMALL
            };
            let body = http_body_util::Limited::new(response.into_body(), max)
                .collect()
                .await
                .map_err(|e| io::Error::other(format!("{name}: reading the body: {e}")))?
                .to_bytes();
            let reply = Reply {
                status,
                body,
                target: name.clone(),
            };
            if !status.is_success()
                && retry::is_transient(status.as_u16(), &xml::error(&reply.body).0)
            {
                return Err(reply.error());
            }
            Ok(reply)
        })
        .await
    }

    /// The object at `key`, or `None` if there is none.
    pub async fn get(&self, key: &str) -> io::Result<Option<Bytes>> {
        let mut call = Call::new(Method::GET, Some(key));
        call.max_reply = MAX_OBJECT;
        let reply = self.send(call).await?;
        match reply.status {
            status if status.is_success() => Ok(Some(reply.body)),
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(reply.error()),
        }
    }

    /// Every key that starts with `prefix`, under the prefix of the store,
    /// without that prefix.
    pub async fn list_prefix(&self, prefix: &str) -> io::Result<Vec<String>> {
        let full = format!("{}{prefix}", self.config.prefix);
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut call = Call::new(Method::GET, None);
            call.query.push(("list-type", "2"));
            call.query.push(("prefix", &full));
            if let Some(token) = &token {
                call.query.push(("continuation-token", token));
            }
            let reply = self.send(call).await?;
            if !reply.status.is_success() {
                return Err(reply.error());
            }
            let page = xml::list_page(&reply.body).ok_or_else(|| {
                io::Error::other(format!("{}: unreadable list page", reply.target))
            })?;
            for key in page.keys {
                let Some(key) = key.strip_prefix(&self.config.prefix) else {
                    return Err(io::Error::other(format!("listed {key} outside the prefix")));
                };
                keys.push(key.to_string());
            }
            match page.next_token {
                Some(next) => token = Some(next),
                None => return Ok(keys),
            }
        }
    }
}

#[async_trait]
impl WalSink for S3Sink {
    /// Stores `wal` at `key`, unless an object is there. An object with the
    /// same bytes counts as stored: it is an earlier attempt of the same
    /// upload.
    async fn put(&self, key: &str, wal: Bytes) -> io::Result<()> {
        let mut call = Call::new(Method::PUT, Some(key));
        call.headers.push(("if-none-match", "*"));
        call.body = wal.clone();
        let reply = self.send(call).await?;
        match reply.status {
            status if status.is_success() => Ok(()),
            StatusCode::PRECONDITION_FAILED => match self.get(key).await? {
                Some(stored) if stored == wal => Ok(()),
                Some(stored) => Err(io::Error::other(format!(
                    "{key} is in the store with other bytes ({} stored, {} new): \
                     two writers disagree on this WAL",
                    stored.len(),
                    wal.len()
                ))),
                None => Err(io::Error::other(format!(
                    "{key}: the store refused the write, then has no object there"
                ))),
            },
            _ => Err(reply.error()),
        }
    }

    async fn list(&self, dir: &str) -> io::Result<Vec<String>> {
        self.list_prefix(&format!("{dir}/")).await
    }

    async fn delete(&self, keys: &[String]) -> io::Result<()> {
        futures_util::stream::iter(keys.iter().cloned())
            .map(|key| async move {
                let reply = self.send(Call::new(Method::DELETE, Some(&key))).await?;
                match reply.status {
                    status if status.is_success() || status == StatusCode::NOT_FOUND => Ok(()),
                    _ => Err(reply.error()),
                }
            })
            .buffer_unordered(DELETES_IN_FLIGHT)
            .try_collect()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: &str, prefix: &str) -> S3Config {
        S3Config {
            endpoint: endpoint.to_string(),
            bucket: "wal bucket".to_string(),
            prefix: prefix.to_string(),
            region: "us-east-1".to_string(),
            access_key: "AK".to_string(),
            secret_key: "secret".to_string(),
            ca_pem: None,
        }
    }

    #[test]
    fn refuses_bad_endpoints_and_prefixes() {
        assert!(S3Sink::new(config("s3.example", "")).is_err());
        assert!(S3Sink::new(config("https://s3.example/path", "")).is_err());
        assert!(S3Sink::new(config("https://s3.example", "wal")).is_err());
        assert!(S3Sink::new(config("https://s3.example:9000/", "wal/")).is_ok());
        let mut crlf = config("https://s3.example", "");
        crlf.access_key = "AK\r".to_string();
        assert!(S3Sink::new(crlf).is_err());
    }

    #[test]
    fn puts_keys_under_the_prefix_and_encodes_them() {
        let sink = S3Sink::new(config("http://127.0.0.1:9000", "db 1/")).unwrap();
        assert_eq!(
            sink.path(Some("7/00000001/a+b")),
            "/wal%20bucket/db%201/7/00000001/a%2Bb"
        );
        assert_eq!(sink.path(None), "/wal%20bucket");
    }

    #[test]
    fn hides_the_secret() {
        let text = format!("{:?}", config("http://h", ""));
        assert!(
            text.contains("<redacted>") && !text.contains("secret\""),
            "{text}"
        );
    }
}
