//! Filesystem helpers for private directories, atomic writes, and file locks.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use fs2::FileExt;

pub(crate) fn ensure_dir(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    if !path.exists() {
        fs::create_dir_all(path)?;
        set_dir_permissions_private(path)?;
    }
    Ok(())
}

/// Write to a temp file then rename to avoid partial writes on crash.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp_path = temp_path_for(path);
    write_file_private(&temp_path, bytes)?;
    replace_atomic(&temp_path, path)?;
    set_file_permissions_private(path)?;
    Ok(())
}

pub(crate) fn append_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let mut file = open_append_file_private(path)?;
    set_file_permissions_private(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

pub(crate) fn replace_atomic(from: &Path, to: &Path) -> io::Result<()> {
    rename_with_fallback(from, to)?;

    if let Some(parent) = to.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        drop(dir.sync_all());
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct FileLock {
    _file: File,
}

impl FileLock {
    pub(crate) fn shared(path: &Path) -> io::Result<Self> {
        let file = open_lock_file(path)?;
        file.lock_shared()?;
        Ok(Self { _file: file })
    }

    pub(crate) fn exclusive(path: &Path) -> io::Result<Self> {
        let file = open_lock_file(path)?;
        file.lock_exclusive()?;
        Ok(Self { _file: file })
    }
}

#[cfg(windows)]
fn rename_with_fallback(from: &Path, to: &Path) -> io::Result<()> {
    if let Err(err) = fs::rename(from, to) {
        if err.kind() == io::ErrorKind::AlreadyExists {
            if to.exists() {
                fs::remove_file(to)?;
            }
            fs::rename(from, to)?;
        } else {
            return Err(err);
        }
    }
    Ok(())
}

#[cfg(not(windows))]
fn rename_with_fallback(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

fn open_lock_file(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

#[cfg(unix)]
fn open_append_file_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_append_file_private(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("private-file");
    path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()))
}

#[cfg(unix)]
fn write_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    fs::write(path, bytes)
}

#[cfg(unix)]
fn set_dir_permissions_private(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = fs::Permissions::from_mode(0o700);
    fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_dir_permissions_private(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions_private(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = fs::Permissions::from_mode(0o600);
    fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_file_permissions_private(_path: &Path) -> io::Result<()> {
    Ok(())
}
