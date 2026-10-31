use std::io;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use pgwal_receiver::WalSink;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// A sink that keeps each object as a file under `root`, at the path of its
/// key.
pub struct DirSink {
    root: PathBuf,
}

impl DirSink {
    pub fn new(root: PathBuf) -> Self {
        DirSink { root }
    }
}

#[async_trait]
impl WalSink for DirSink {
    /// Writes a temporary file, syncs it and renames it, so a crash never
    /// leaves a partial object under its key.
    async fn put(&self, key: &str, wal: Bytes) -> io::Result<()> {
        let path = self.root.join(key);
        let dir = path.parent().expect("a key has a parent directory");
        let name = path.file_name().expect("a key has a file name");
        fs::create_dir_all(dir).await?;
        let tmp = dir.join(format!(".{}.tmp", name.to_string_lossy()));
        let mut file = fs::File::create(&tmp).await?;
        file.write_all(&wal).await?;
        file.sync_all().await?;
        fs::rename(&tmp, &path).await?;
        fs::File::open(dir).await?.sync_all().await
    }

    async fn list(&self, dir: &str) -> io::Result<Vec<String>> {
        let mut entries = match fs::read_dir(self.root.join(dir)).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut keys = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with('.') {
                keys.push(format!("{dir}/{name}"));
            }
        }
        Ok(keys)
    }

    async fn delete(&self, keys: &[String]) -> io::Result<()> {
        for key in keys {
            match fs::remove_file(self.root.join(key)).await {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_list_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = DirSink::new(tmp.path().to_path_buf());
        sink.put("7/00000001/a", Bytes::from_static(b"wal"))
            .await
            .unwrap();
        assert_eq!(sink.list("7/00000001").await.unwrap(), ["7/00000001/a"]);
        assert_eq!(
            std::fs::read(tmp.path().join("7/00000001/a")).unwrap(),
            b"wal"
        );
        sink.delete(&["7/00000001/a".into(), "7/00000001/gone".into()])
            .await
            .unwrap();
        assert!(sink.list("7/00000001").await.unwrap().is_empty());
        assert!(sink.list("8/00000001").await.unwrap().is_empty());
    }
}
