//! Persisted refresh-token storage. Atomic writes (write-temp + rename) so a crash
//! mid-write can't corrupt the file or lose the rotated refresh token.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// `access_token`'s expiry, as a UNIX-epoch timestamp in seconds.
    pub access_expires_at: u64,
}

impl StoredTokens {
    pub fn access_expires(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(self.access_expires_at)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TokenStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("token-store file is corrupt: {0}")]
    Parse(serde_json::Error),
}

pub struct TokenStore {
    path: PathBuf,
}

impl TokenStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<StoredTokens>, TokenStoreError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(TokenStoreError::Parse)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save(&self, tokens: &StoredTokens) -> Result<(), TokenStoreError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut tmp = self.path.clone();
        tmp.set_extension("tmp");
        write_atomic(&tmp, &self.path, tokens)?;
        Ok(())
    }
}

#[cfg(unix)]
fn write_atomic(tmp: &Path, dst: &Path, tokens: &StoredTokens) -> Result<(), TokenStoreError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true).create(true).truncate(true).mode(0o600)
        .open(tmp)?;
    serde_json::to_writer(&mut f, tokens).map_err(TokenStoreError::Parse)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(tmp, dst)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_atomic(tmp: &Path, dst: &Path, tokens: &StoredTokens) -> Result<(), TokenStoreError> {
    let mut f = std::fs::OpenOptions::new()
        .write(true).create(true).truncate(true)
        .open(tmp)?;
    serde_json::to_writer(&mut f, tokens).map_err(TokenStoreError::Parse)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(tmp, dst)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StoredTokens {
        StoredTokens {
            access_token: "a.b.c".to_string(),
            refresh_token: "r.r.r".to_string(),
            access_expires_at: 1_777_780_278,
        }
    }

    #[test]
    fn load_returns_none_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path().join("tokens.json"));
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path().join("tokens.json"));
        store.save(&sample()).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, sample());
    }

    #[test]
    fn save_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path().join("nested/dir/tokens.json"));
        store.save(&sample()).unwrap();
        assert!(dir.path().join("nested/dir/tokens.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn save_writes_with_0600_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        let store = TokenStore::new(&path);
        store.save(&sample()).unwrap();
        let perms = std::fs::metadata(&path).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o600);
    }

    #[test]
    fn corrupt_file_yields_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        std::fs::write(&path, "not json").unwrap();
        let store = TokenStore::new(&path);
        assert!(matches!(store.load(), Err(TokenStoreError::Parse(_))));
    }
}
