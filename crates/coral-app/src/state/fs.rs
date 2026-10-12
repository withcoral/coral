//! Private-directory and atomic marker-write helpers for app state.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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

pub(crate) fn ensure_private_dir(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    fs::create_dir_all(path)?;
    set_dir_permissions_private(path)?;
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

fn replace_atomic(from: &Path, to: &Path) -> io::Result<()> {
    rename_with_fallback(from, to)?;

    if let Some(parent) = to.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        drop(dir.sync_all());
    }

    Ok(())
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

fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("private-file");
    path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()))
}

/// Opens `path` for reading and writing, creating it readable only by its
/// owner on Unix when it does not exist. Existing contents are kept.
pub(crate) fn open_private_file(path: &Path) -> io::Result<fs::File> {
    private_file_options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn write_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;

    let mut file = private_file_options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Options whose created files only their owner can read on Unix.
fn private_file_options() -> fs::OpenOptions {
    #[cfg_attr(not(unix), expect(unused_mut, reason = "only Unix sets a mode"))]
    let mut options = fs::OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
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
