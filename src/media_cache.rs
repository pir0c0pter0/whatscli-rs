use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneReport {
    pub expired: usize,
    pub evicted: usize,
    pub bytes_remaining: u64,
}

pub async fn prune(path: PathBuf, retention_days: u64, max_mb: u64) -> Result<PruneReport> {
    tokio::task::spawn_blocking(move || prune_blocking(&path, retention_days, max_mb))
        .await
        .map_err(|error| anyhow!("media cache cleanup failed: {error}"))?
}

fn prune_blocking(path: &Path, retention_days: u64, max_mb: u64) -> Result<PruneReport> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create media cache {}", path.display()))?;
    let now = SystemTime::now();
    let cutoff = now
        .checked_sub(Duration::from_secs(retention_days.saturating_mul(86_400)))
        .unwrap_or(UNIX_EPOCH);
    let mut files = Vec::new();
    let mut report = PruneReport::default();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        let accessed = metadata
            .accessed()
            .or_else(|_| metadata.modified())
            .unwrap_or(now);
        if accessed < cutoff {
            std::fs::remove_file(entry.path())?;
            report.expired += 1;
        } else {
            files.push((entry.path(), accessed, metadata.len()));
        }
    }
    files.sort_by_key(|(_, accessed, _)| *accessed);
    let limit = max_mb.saturating_mul(1024 * 1024);
    let mut total = files.iter().map(|(_, _, size)| size).sum::<u64>();
    for (path, _, size) in files {
        if total <= limit {
            break;
        }
        std::fs::remove_file(path)?;
        total = total.saturating_sub(size);
        report.evicted += 1;
    }
    report.bytes_remaining = total;
    Ok(report)
}

pub fn safe_file_name(candidate: &str, fallback: &str) -> String {
    let normalized = candidate.replace('\\', "/");
    let leaf = normalized.rsplit('/').next().unwrap_or_default().trim();
    let cleaned: String = leaf
        .chars()
        .map(|ch| {
            if ch.is_control() || matches!(ch, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
            {
                '_'
            } else {
                ch
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches([' ', '.']);
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        let safe_fallback: String = fallback
            .chars()
            .map(|ch| {
                if ch.is_control() || matches!(ch, '/' | '\\') {
                    '_'
                } else {
                    ch
                }
            })
            .collect();
        let safe_fallback = safe_fallback.trim_matches([' ', '.']);
        if safe_fallback.is_empty() {
            "media".to_owned()
        } else {
            safe_fallback.chars().take(180).collect()
        }
    } else {
        cleaned.chars().take(180).collect()
    }
}

pub async fn cache_path(base: &Path, id: &str, file_name: &str) -> Result<PathBuf> {
    tokio::fs::create_dir_all(base).await?;
    let fallback = safe_file_name(id, "media");
    let name = safe_file_name(file_name, &fallback);
    Ok(base.join(format!("{}-{name}", safe_file_name(id, "media"))))
}

pub async fn touch(path: &Path) -> Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let now = filetime::FileTime::now();
        filetime::set_file_times(&path, now, now)
            .with_context(|| format!("failed to touch {}", path.display()))
    })
    .await
    .map_err(|error| anyhow!("media cache touch failed: {error}"))?
}

pub async fn atomic_write_cached(path: &Path, data: &[u8]) -> Result<PathBuf> {
    if tokio::fs::try_exists(path).await? {
        touch(path).await?;
        return Ok(path.to_owned());
    }
    match atomic_install(path, data).await {
        Ok(()) => {}
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == ErrorKind::AlreadyExists) =>
        {
            touch(path).await?;
        }
        Err(error) => return Err(error),
    }
    Ok(path.to_owned())
}

pub async fn atomic_write_download(dir: &Path, desired: &str, data: &[u8]) -> Result<PathBuf> {
    tokio::fs::create_dir_all(dir).await?;
    let desired = safe_file_name(desired, "media");
    let stem = Path::new(&desired)
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("media");
    let extension = Path::new(&desired).extension().and_then(OsStr::to_str);
    for suffix in 0..10_000_u32 {
        let name = if suffix == 0 {
            desired.clone()
        } else if let Some(extension) = extension {
            format!("{stem} ({suffix}).{extension}")
        } else {
            format!("{stem} ({suffix})")
        };
        let target = dir.join(name);
        match atomic_install(&target, data).await {
            Ok(()) => return Ok(target),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == ErrorKind::AlreadyExists) => {}
            Err(error) => return Err(error),
        }
    }
    Err(anyhow!("could not choose an unused download name"))
}

async fn atomic_install(target: &Path, data: &[u8]) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("media path has no parent"))?;
    tokio::fs::create_dir_all(parent).await?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp = parent.join(format!(".whatscli-{}-{nonce}.part", std::process::id()));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(&temp).await?;
    use tokio::io::AsyncWriteExt;
    file.write_all(data).await?;
    file.flush().await?;
    file.sync_all().await?;
    drop(file);
    let install = tokio::fs::hard_link(&temp, target).await;
    let _ = tokio::fs::remove_file(&temp).await;
    install.map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("whatscli-media-{label}-{}", std::process::id()))
    }

    #[tokio::test]
    async fn downloads_are_atomic_sanitized_and_never_overwritten() {
        let dir = temp_dir("download");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let first = atomic_write_download(&dir, "../../hostile?.webp", b"one")
            .await
            .unwrap();
        let second = atomic_write_download(&dir, "../../hostile?.webp", b"two")
            .await
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(tokio::fs::read(first).await.unwrap(), b"one");
        assert_eq!(tokio::fs::read(second).await.unwrap(), b"two");
        assert!(
            tokio::fs::read_dir(&dir)
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_some()
        );
        let _ = tokio::fs::remove_dir_all(dir).await;
    }

    #[tokio::test]
    async fn cache_hit_keeps_existing_bytes() {
        let dir = temp_dir("hit");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let path = dir.join("item.webp");
        atomic_write_cached(&path, b"one").await.unwrap();
        atomic_write_cached(&path, b"two").await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"one");
        let _ = tokio::fs::remove_dir_all(dir).await;
    }

    #[tokio::test]
    async fn prune_removes_expired_then_oldest_until_under_limit() {
        let dir = temp_dir("prune");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let expired = dir.join("expired");
        let older = dir.join("older");
        let newer = dir.join("newer");
        tokio::fs::write(&expired, vec![0; 32]).await.unwrap();
        tokio::fs::write(&older, vec![0; 700_000]).await.unwrap();
        tokio::fs::write(&newer, vec![0; 700_000]).await.unwrap();
        let now = filetime::FileTime::now();
        let very_old = filetime::FileTime::from_unix_time(now.unix_seconds() - 40 * 86_400, 0);
        let old = filetime::FileTime::from_unix_time(now.unix_seconds() - 60, 0);
        filetime::set_file_times(&expired, very_old, very_old).unwrap();
        filetime::set_file_times(&older, old, old).unwrap();
        let report = prune(dir.clone(), 30, 1).await.unwrap();
        assert_eq!(report.expired, 1);
        assert_eq!(report.evicted, 1);
        assert!(!tokio::fs::try_exists(expired).await.unwrap());
        assert!(!tokio::fs::try_exists(older).await.unwrap());
        assert!(tokio::fs::try_exists(newer).await.unwrap());
        let _ = tokio::fs::remove_dir_all(dir).await;
    }
}
