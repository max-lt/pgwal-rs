use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Args;
use pgwal_receiver::{DirSink, WalSink};

/// The archive of `pgwal receive`.
#[derive(Args, Debug)]
pub struct ArchiveArgs {
    /// The archive: a directory, or `s3://bucket/prefix/`.
    #[arg(long)]
    archive: String,
    #[command(flatten)]
    s3: S3Args,
}

impl ArchiveArgs {
    pub fn open(&self) -> Result<Arc<dyn WalSink>> {
        let Some(location) = self.archive.strip_prefix("s3://") else {
            return Ok(Arc::new(DirSink::new(&self.archive)));
        };
        open_s3(location, &self.s3)
    }
}

/// Where to reach an S3 archive. The keys come from `AWS_ACCESS_KEY_ID` and
/// `AWS_SECRET_ACCESS_KEY` only.
#[derive(Args, Debug)]
struct S3Args {
    /// `https://host[:port]` of the S3 store. Without it, AWS in the region.
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    s3_region: String,
    /// PEM file of CAs to trust next to the public roots.
    #[arg(long, env = "AWS_CA_BUNDLE")]
    s3_ca_file: Option<PathBuf>,
}

/// The object at `key`, which must be there.
pub async fn read(archive: &dyn WalSink, key: &str) -> Result<Bytes> {
    archive
        .get(key)
        .await
        .with_context(|| format!("reading archived object {key}"))?
        .with_context(|| format!("archived object {key} is missing"))
}

#[cfg(feature = "s3")]
fn open_s3(location: &str, s3: &S3Args) -> Result<Arc<dyn WalSink>> {
    let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
    let prefix = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    };
    let env =
        |name: &str| std::env::var(name).with_context(|| format!("an s3:// archive needs {name}"));
    let ca_pem = match &s3.s3_ca_file {
        Some(path) => {
            Some(std::fs::read(path).with_context(|| format!("reading {}", path.display()))?)
        }
        None => None,
    };
    let config = pgwal_receiver::s3::S3Config {
        endpoint: s3
            .s3_endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", s3.s3_region)),
        bucket: bucket.to_string(),
        prefix,
        region: s3.s3_region.clone(),
        access_key: env("AWS_ACCESS_KEY_ID")?,
        secret_key: env("AWS_SECRET_ACCESS_KEY")?,
        ca_pem,
    };
    Ok(Arc::new(pgwal_receiver::s3::S3Sink::new(config)?))
}

#[cfg(not(feature = "s3"))]
fn open_s3(_location: &str, _s3: &S3Args) -> Result<Arc<dyn WalSink>> {
    anyhow::bail!("this pgwal is built without the s3 feature")
}
