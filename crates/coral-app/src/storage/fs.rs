//! Filesystem helpers for private directories, atomic writes, and file locks.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use uuid::Uuid;

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

/// Creates or tightens one private directory entry without accepting a symlink.
///
/// Missing ancestors are checked and created up to the first existing directory,
/// which is the caller-owned trust boundary. Callers must validate that boundary
/// separately when it is not already trusted; walking every absolute ancestor
/// would reject platform paths with standard symlink prefixes such as macOS `/var`.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "a later stack layer activates strict directory creation for local encryption keys"
    )
)]
pub(crate) fn ensure_private_dir_no_symlink(path: &Path) -> io::Result<()> {
    ensure_private_dir_no_symlink_inner(path, true)
}

fn ensure_private_dir_no_symlink_inner(path: &Path, tighten_existing: bool) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            if !tighten_existing {
                // Do not chmod or walk above the caller's first existing directory.
                return Ok(());
            }
        }
        Ok(_) => return Err(private_directory_error(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                ensure_private_dir_no_symlink_inner(parent, false)?;
            }
            fs::create_dir(path)?;
        }
        Err(error) => return Err(error),
    }
    tighten_private_dir_no_symlink(path)
}

fn ensure_existing_private_dir_no_symlink(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => tighten_private_dir_no_symlink(path),
        Ok(_) => Err(private_directory_error(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(private_directory_error(path)),
        Err(error) => Err(error),
    }
}

fn tighten_private_dir_no_symlink(path: &Path) -> io::Result<()> {
    let dir = File::open(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            private_directory_error(path)
        } else {
            error
        }
    })?;
    set_open_dir_permissions_private(&dir)
}

pub(crate) fn create_new_file_private(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    open_create_new_file_private(path)
}

pub(crate) fn ensure_file_private(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    match open_create_new_file_private(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            ensure_existing_file_private(path)
        }
        Err(error) => Err(error),
    }
}

/// Reads a bounded private regular file, tightening its permissions first.
///
/// Rejects symlinks and non-regular entries and caps the read, so a stray or
/// swapped path cannot become an unbounded read. It deliberately does not defend
/// against a path swapped *during* the read: reaching the key file's `0700` parent
/// requires write access to it, and anything able to write there can generally read
/// the key outright. The value here is tightening loose modes, which is a real and
/// common failure, not defeating a local attacker who already has the directory.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "a later stack layer activates strict reads for local encryption keys"
    )
)]
pub(crate) fn read_to_string_private(path: &Path, max_bytes: u64) -> io::Result<String> {
    if let Some(parent) = path.parent() {
        ensure_existing_private_dir_no_symlink(parent)?;
    }
    let path_metadata = fs::symlink_metadata(path)?;
    if !path_metadata.file_type().is_file() {
        return Err(not_same_regular_file(path));
    }

    let mut file = File::open(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            not_same_regular_file(path)
        } else {
            error
        }
    })?;
    set_open_permissions_private(&file)?;

    let mut raw = String::new();
    std::io::Read::by_ref(&mut file)
        .take(max_bytes.saturating_add(1))
        .read_to_string(&mut raw)?;
    if raw.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("private file exceeds {max_bytes} bytes: {}", path.display()),
        ));
    }
    Ok(raw)
}

fn not_same_regular_file(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "path is not the same private regular file that was inspected: {}",
            path.display()
        ),
    )
}

fn private_directory_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("path is not a private directory: {}", path.display()),
    )
}

fn ensure_existing_file_private(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("path exists and is not a regular file: {}", path.display()),
        ));
    }
    set_file_permissions_private(path)
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
    file.sync_all()?;
    // Best-effort: try to durably link the (possibly freshly created) file into
    // its directory so a crash is less likely to lose it. Like `replace_atomic`,
    // the parent-directory fsync is best-effort — opening or fsyncing a directory
    // is not portable (e.g. it fails on Windows), so a failure here is ignored
    // rather than surfaced.
    if let Some(parent) = path.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        drop(dir.sync_all());
    }
    Ok(())
}

pub(crate) fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
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
pub(crate) struct DirectoryBackup {
    original: PathBuf,
    backup: PathBuf,
    moved: bool,
}

impl DirectoryBackup {
    pub(crate) fn move_for_delete(path: &Path, name: impl fmt::Display) -> io::Result<Self> {
        let backup = path.with_file_name(format!("{name}.delete.rollback.{}", Uuid::new_v4()));
        if !path.try_exists()? {
            return Ok(Self {
                original: path.to_path_buf(),
                backup,
                moved: false,
            });
        }
        if backup.try_exists()? {
            fs::remove_dir_all(&backup)?;
        }
        fs::rename(path, &backup)?;
        Ok(Self {
            original: path.to_path_buf(),
            backup,
            moved: true,
        })
    }

    pub(crate) fn backup_path(&self) -> &Path {
        &self.backup
    }

    pub(crate) fn restore(&self) -> io::Result<()> {
        if self.moved && self.backup.try_exists()? {
            if self.original.try_exists()? {
                fs::remove_dir_all(&self.original)?;
            }
            fs::rename(&self.backup, &self.original)?;
        }
        Ok(())
    }

    pub(crate) fn commit(&self) -> io::Result<()> {
        if self.moved && self.backup.try_exists()? {
            fs::remove_dir_all(&self.backup)?;
        }
        Ok(())
    }
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

#[cfg(unix)]
fn open_create_new_file_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_create_new_file_private(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
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

#[cfg(unix)]
fn set_open_permissions_private(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    file.set_permissions(fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_open_permissions_private(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_open_dir_permissions_private(directory: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    directory.set_permissions(fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_open_dir_permissions_private(_directory: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DirectoryBackup, ensure_file_private, ensure_private_dir_no_symlink,
        read_to_string_private, remove_file_if_exists,
    };

    #[test]
    fn read_to_string_private_rejects_non_regular_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("credential-encryption.key");
        std::fs::create_dir(&path).expect("create directory at key path");

        let error = read_to_string_private(&path, 1024).expect_err("directory should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("private regular file"));
    }

    #[test]
    #[cfg(unix)]
    fn read_to_string_private_rejects_symlink_without_chmoding_target() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = tempfile::tempdir().expect("tempdir");
        let target = temp.path().join("target.key");
        let link = temp.path().join("credential-encryption.key");
        std::fs::write(&target, "v1:not-a-real-key\n").expect("write target");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))
            .expect("set target permissions");
        symlink(&target, &link).expect("create symlink");

        let error = read_to_string_private(&link, 1024).expect_err("symlink should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        let target_mode = std::fs::metadata(&target)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(target_mode, 0o644);
    }

    #[test]
    #[cfg(unix)]
    fn read_to_string_private_tightens_opened_file_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("credential-encryption.key");
        std::fs::write(&path, "private key\n").expect("write key");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("set permissive mode");
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o755))
            .expect("set permissive parent mode");

        assert_eq!(
            read_to_string_private(&path, 1024).expect("read key"),
            "private key\n"
        );
        let mode = std::fs::metadata(&path)
            .expect("key metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        let parent_mode = std::fs::metadata(temp.path())
            .expect("parent metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(parent_mode, 0o700);
    }

    #[test]
    #[cfg(unix)]
    fn read_to_string_private_rejects_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let target = temp.path().join("target");
        std::fs::create_dir(&target).expect("target directory");
        std::fs::write(target.join("encryption.key"), "private key\n").expect("key");
        let parent = temp.path().join("credentials");
        symlink(&target, &parent).expect("parent symlink");

        let error = read_to_string_private(&parent.join("encryption.key"), 1024)
            .expect_err("symlinked parent should be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    #[cfg(unix)]
    fn ensure_private_dir_no_symlink_rejects_missing_directory_beneath_symlink() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = tempfile::tempdir().expect("tempdir");
        let target = temp.path().join("target");
        std::fs::create_dir(&target).expect("target directory");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
            .expect("set target permissions");
        let config = temp.path().join("config");
        symlink(&target, &config).expect("config symlink");

        let error = ensure_private_dir_no_symlink(&config.join("credentials"))
            .expect_err("symlinked ancestor should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(!target.join("credentials").exists());
        let target_mode = std::fs::metadata(&target)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(target_mode, 0o755);
    }

    #[test]
    #[cfg(unix)]
    fn ensure_private_dir_no_symlink_does_not_tighten_existing_ancestor() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o755))
            .expect("set ancestor permissions");
        let private = temp.path().join("private");
        let credentials = private.join("credentials");

        ensure_private_dir_no_symlink(&credentials).expect("create private directories");
        for path in [&private, &credentials] {
            let mode = std::fs::metadata(path)
                .expect("created private directory")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700);
        }
        let ancestor_mode = std::fs::metadata(temp.path())
            .expect("ancestor metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(ancestor_mode, 0o755);
    }

    #[test]
    fn read_to_string_private_does_not_recreate_a_missing_parent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let parent = temp.path().join("credentials");

        let error = read_to_string_private(&parent.join("encryption.key"), 1024)
            .expect_err("missing parent should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(!parent.exists());
    }

    #[test]
    fn read_to_string_private_rejects_oversized_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("encryption.key");
        std::fs::write(&path, "0123456789").expect("key");

        let error = read_to_string_private(&path, 9).expect_err("oversized file");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn ensure_file_private_rejects_existing_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("coral.db");
        std::fs::create_dir(&path).expect("create directory at file path");

        let error = ensure_file_private(&path).expect_err("directory should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            error.to_string().contains("not a regular file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn ensure_file_private_rejects_symlink_without_chmoding_target() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = tempfile::tempdir().expect("tempdir");
        let target = temp.path().join("target.db");
        let link = temp.path().join("coral.db");
        std::fs::write(&target, "existing database").expect("write target");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))
            .expect("set target permissions");
        symlink(&target, &link).expect("create symlink");

        let error = ensure_file_private(&link).expect_err("symlink should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            error.to_string().contains("not a regular file"),
            "unexpected error: {error}"
        );
        let target_mode = std::fs::metadata(&target)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(target_mode, 0o644);
    }

    #[test]
    fn directory_backup_moves_and_restores_delete_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("github");
        std::fs::create_dir(&source_dir).expect("create source dir");
        std::fs::write(source_dir.join("manifest.yaml"), "name: github\n").expect("write file");

        let backup = DirectoryBackup::move_for_delete(&source_dir, "github").expect("move backup");

        assert!(!source_dir.exists());
        assert!(backup.backup_path().exists());
        assert!(
            backup
                .backup_path()
                .file_name()
                .expect("backup filename")
                .to_string_lossy()
                .starts_with("github.delete.rollback.")
        );

        backup.restore().expect("restore backup");

        assert!(source_dir.join("manifest.yaml").exists());
        assert!(!backup.backup_path().exists());
    }

    #[test]
    fn directory_backup_commits_delete_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace_dir = temp.path().join("workspace");
        std::fs::create_dir(&workspace_dir).expect("create workspace dir");

        let backup =
            DirectoryBackup::move_for_delete(&workspace_dir, "workspace").expect("move backup");
        backup.commit().expect("commit backup");

        assert!(!workspace_dir.exists());
        assert!(!backup.backup_path().exists());
    }

    #[test]
    fn directory_backup_noops_for_missing_delete_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let missing_dir = temp.path().join("missing");

        let backup =
            DirectoryBackup::move_for_delete(&missing_dir, "missing").expect("prepare backup");
        backup.restore().expect("restore missing");
        backup.commit().expect("commit missing");

        assert!(!missing_dir.exists());
        assert!(!backup.backup_path().exists());
    }

    #[test]
    fn remove_file_if_exists_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("legacy.jsonl");
        std::fs::write(&path, "legacy").expect("write legacy file");

        remove_file_if_exists(&path).expect("remove existing file");
        remove_file_if_exists(&path).expect("ignore missing file");

        assert!(!path.exists());
    }
}
