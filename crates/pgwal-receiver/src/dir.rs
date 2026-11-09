//! A [`WalSink`] in a local directory.

use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::WalSink;

/// Keeps each object as a file under `root`, at the path of its key. A file
/// whose name starts with `.` is a put in progress.
pub struct DirSink {
    root: PathBuf,
}

impl DirSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        DirSink { root: root.into() }
    }
}

#[async_trait]
impl WalSink for DirSink {
    /// Writes a temporary file, syncs it and renames it, so a crash never
    /// leaves a partial object under its key.
    async fn put(&self, key: &str, data: Bytes) -> io::Result<()> {
        let path = self.root.join(key);
        write_durably(&path, &data).await.map_err(at(&path))
    }

    async fn get(&self, key: &str) -> io::Result<Option<Bytes>> {
        match fs::read(self.root.join(key)).await {
            Ok(data) => Ok(Some(data.into())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        // The walk starts at the deepest directory that the prefix names.
        let start = prefix.rsplit_once('/').map_or("", |(dir, _)| dir);
        let mut keys = Vec::new();
        let mut dirs = vec![start.to_string()];
        while let Some(dir) = dirs.pop() {
            let path = self.root.join(&dir);
            let mut entries = match fs::read_dir(&path).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(at(&path)(e)),
            };
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if name.starts_with('.') {
                    continue;
                }
                let key = if dir.is_empty() {
                    name.to_string()
                } else {
                    format!("{dir}/{name}")
                };
                if entry.file_type().await?.is_dir() {
                    dirs.push(key);
                } else if key.starts_with(prefix) {
                    keys.push(key);
                }
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

async fn write_durably(path: &Path, data: &[u8]) -> io::Result<()> {
    let dir = path.parent().expect("a key has a parent directory");
    let name = path.file_name().expect("a key has a file name");
    fs::create_dir_all(dir).await?;
    let tmp = dir.join(format!(".{}.tmp", name.to_string_lossy()));
    let mut file = fs::File::create(&tmp).await?;
    file.write_all(data).await?;
    // The write ends on a blocking thread: only a flush gives its error.
    file.flush().await?;
    file.sync_all().await?;
    fs::rename(&tmp, path).await?;
    fs::File::open(dir).await?.sync_all().await
}

fn at(path: &Path) -> impl FnOnce(io::Error) -> io::Error + '_ {
    move |e| io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_list_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = DirSink::new(tmp.path());
        for key in ["7/00000001/0A-0B", "7/00000001/1A-1B", "7/base/x/done"] {
            sink.put(key, Bytes::from_static(b"wal")).await.unwrap();
        }
        assert_eq!(sink.get("7/00000001/0A-0B").await.unwrap().unwrap(), "wal");
        assert_eq!(sink.get("7/00000001/gone").await.unwrap(), None);
        assert_eq!(
            sink.list("7/00000001/0").await.unwrap(),
            ["7/00000001/0A-0B"]
        );
        let mut all = sink.list("").await.unwrap();
        all.sort();
        assert_eq!(
            all,
            ["7/00000001/0A-0B", "7/00000001/1A-1B", "7/base/x/done"]
        );
        sink.delete(&["7/00000001/0A-0B".into(), "7/00000001/gone".into()])
            .await
            .unwrap();
        assert_eq!(
            sink.list("7/00000001/").await.unwrap(),
            ["7/00000001/1A-1B"]
        );
        assert!(sink.list("8/").await.unwrap().is_empty());
    }
}
