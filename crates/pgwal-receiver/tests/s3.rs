//! Runs the S3 sink against a real S3 store.
//!
//! Skipped unless `PGWAL_TEST_S3_ENDPOINT` and `PGWAL_TEST_S3_BUCKET` are
//! set, with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. `AWS_REGION`
//! defaults to `us-east-1`. `AWS_CA_BUNDLE` adds a PEM file of CAs to trust.
//! Each run works under its own prefix and deletes what it wrote.

#![cfg(feature = "s3")]

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use pgwal_receiver::WalSink;
use pgwal_receiver::s3::{S3Config, S3Sink};

fn sink(prefix: &str) -> Option<S3Sink> {
    let endpoint = std::env::var("PGWAL_TEST_S3_ENDPOINT").ok()?;
    let bucket = std::env::var("PGWAL_TEST_S3_BUCKET").ok()?;
    let ca_pem = std::env::var("AWS_CA_BUNDLE")
        .ok()
        .map(|path| std::fs::read(path).unwrap());
    let mut config = S3Config::new(
        endpoint,
        bucket,
        std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
        std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
        std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
    );
    config.prefix = prefix.to_string();
    config.ca_pem = ca_pem;
    Some(S3Sink::new(config).unwrap())
}

fn run_prefix(test: &str) -> String {
    format!("pgwal-test-{test}-{}/", std::process::id())
}

#[tokio::test]
async fn put_get_list_delete_and_a_second_put() {
    let Some(sink) = sink(&run_prefix("basic")) else {
        eprintln!("skipping: no PGWAL_TEST_S3_ENDPOINT");
        return;
    };
    let key = "7401/00000001/0000000001000000-0000000001000100";
    let wal = Bytes::from(vec![7u8; 256]);
    sink.put(key, wal.clone()).await.unwrap();
    assert_eq!(sink.get(key).await.unwrap(), Some(wal.clone()));
    assert_eq!(sink.list("7401/00000001").await.unwrap(), [key]);

    // The same key with the same bytes: an earlier attempt of this upload.
    sink.put(key, wal.clone()).await.unwrap();
    // The same key with other bytes: never overwritten.
    let error = sink
        .put(key, Bytes::from(vec![8u8; 256]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("other bytes"), "{error}");
    assert_eq!(sink.get(key).await.unwrap(), Some(wal));

    sink.delete(&[key.to_string(), "7401/00000001/gone".to_string()])
        .await
        .unwrap();
    assert!(sink.list("7401/00000001").await.unwrap().is_empty());
    assert_eq!(sink.get(key).await.unwrap(), None);
}

/// A listing longer than one page, and a delete of many keys.
#[tokio::test]
async fn lists_and_deletes_past_one_page() {
    let Some(sink) = sink(&run_prefix("pages")) else {
        eprintln!("skipping: no PGWAL_TEST_S3_ENDPOINT");
        return;
    };
    let keys: Vec<String> = (0..1100)
        .map(|i| format!("7401/00000001/{i:016X}-{:016X}", i + 1))
        .collect();
    futures_util::stream::iter(keys.iter().cloned())
        .map(|key| {
            let sink = &sink;
            async move { sink.put(&key, Bytes::from_static(b"w")).await }
        })
        .buffer_unordered(32)
        .try_collect::<()>()
        .await
        .unwrap();
    let mut listed = sink.list("7401/00000001").await.unwrap();
    listed.sort();
    assert_eq!(listed, keys);
    sink.delete(&keys).await.unwrap();
    assert!(sink.list("7401/00000001").await.unwrap().is_empty());
}
