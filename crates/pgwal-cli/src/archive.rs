use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Args;
use pgwal_receiver::WalSink;

use crate::sink::DirSink;

/// Where to reach an S3 archive, `--archive s3://bucket/prefix/`. The keys
/// come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` only.
#[derive(Args, Debug, Clone)]
pub struct S3Args {
    /// `https://host[:port]` of the S3 store. Without it, AWS in the region.
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    s3_region: String,
    /// PEM file of CAs to trust next to the public roots.
    #[arg(long, env = "AWS_CA_BUNDLE")]
    s3_ca_file: Option<PathBuf>,
}

/// An archive in a directory, or in an S3 bucket.
pub enum Archive {
    Dir(PathBuf),
    #[cfg(feature = "s3")]
    S3(Arc<pgwal_receiver::s3::S3Sink>),
}

impl Archive {
    /// `spec` is a directory, or `s3://bucket/prefix/`.
    pub fn open(spec: &str, s3: &S3Args) -> Result<Archive> {
        let Some(location) = spec.strip_prefix("s3://") else {
            return Ok(Archive::Dir(spec.into()));
        };
        open_s3(location, s3)
    }

    pub fn sink(&self) -> Arc<dyn WalSink> {
        match self {
            Archive::Dir(root) => Arc::new(DirSink::new(root.clone())),
            #[cfg(feature = "s3")]
            Archive::S3(sink) => sink.clone(),
        }
    }

    /// Every key of the archive.
    pub async fn keys(&self) -> Result<Vec<String>> {
        match self {
            Archive::Dir(root) => dir_keys(root),
            #[cfg(feature = "s3")]
            Archive::S3(sink) => Ok(sink.list_prefix("").await?),
        }
    }

    pub async fn read(&self, key: &str) -> Result<Bytes> {
        let read = match self {
            Archive::Dir(root) => tokio::fs::read(root.join(key)).await.map(Bytes::from),
            #[cfg(feature = "s3")]
            Archive::S3(sink) => sink.get(key).await.and_then(|object| {
                object.ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
            }),
        };
        read.with_context(|| format!("reading archived object {key}"))
    }
}

#[cfg(feature = "s3")]
fn open_s3(location: &str, s3: &S3Args) -> Result<Archive> {
    let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
    let prefix = match prefix.is_empty() || prefix.ends_with('/') {
        true => prefix.to_string(),
        false => format!("{prefix}/"),
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
        retry: pgwal_receiver::s3::Retry::default(),
    };
    Ok(Archive::S3(Arc::new(pgwal_receiver::s3::S3Sink::new(
        config,
    )?)))
}

#[cfg(not(feature = "s3"))]
fn open_s3(_location: &str, _s3: &S3Args) -> Result<Archive> {
    anyhow::bail!("this pgwal is built without the s3 feature")
}

/// The file paths under `root`. Names that start with `.` are files that an
/// upload has not finished.
fn dir_keys(root: &Path) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let entries = std::fs::read_dir(root.join(&dir))
            .with_context(|| format!("reading {}", root.join(&dir).display()))?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with('.') {
                continue;
            }
            let path = dir.join(name);
            if entry.file_type()?.is_dir() {
                dirs.push(path);
            } else if let Some(key) = path.to_str() {
                keys.push(key.to_owned());
            }
        }
    }
    Ok(keys)
}
