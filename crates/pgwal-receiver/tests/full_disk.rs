//! A write that fails after `write_all` returns, as on a full disk. The file
//! size limit applies to the whole process, so this test has its own binary.

use bytes::Bytes;
use pgwal_receiver::{DirSink, WalSink};

/// Writes past `bytes` fail with EFBIG, as writes fail with ENOSPC on a full
/// disk.
fn limit_file_size(bytes: libc::rlim_t) {
    // SAFETY: the calls change the limits of this process only.
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut limit), 0);
        limit.rlim_cur = bytes.min(limit.rlim_max);
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limit), 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_put_that_fails_to_write_fails_and_leaves_no_object() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = DirSink::new(tmp.path());
    limit_file_size(4096);
    let put = sink
        .put("7/00000001/k", Bytes::from(vec![1; 100_000]))
        .await;
    limit_file_size(libc::RLIM_INFINITY);
    assert!(put.is_err(), "the put passed");
    assert_eq!(sink.get("7/00000001/k").await.unwrap(), None);
}
