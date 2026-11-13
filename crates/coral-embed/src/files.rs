//! Finding the manifest's files on disk.

use std::fs::File;
use std::io::{self, BufRead as _, BufReader};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

use crate::manifest::{Manifest, ModelFile, hex};

/// The read-only folder of model folders on Linux. The server image puts
/// the manifest's files in its `<model id>` folder. The operator of a host
/// without network access can put them there too.
pub const SYSTEM_MODELS_DIR: &str = "/opt/coral/models";

/// The folders that hold model folders, in the order Coral looks in them:
/// on Linux [`SYSTEM_MODELS_DIR`], then `models` in the state directory.
#[must_use]
pub fn models_dirs(state_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::with_capacity(2);
    if cfg!(target_os = "linux") {
        dirs.push(PathBuf::from(SYSTEM_MODELS_DIR));
    }
    dirs.push(state_dir.join("models"));
    dirs
}

/// Whether the manifest's files are on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelState {
    /// This folder holds every file of the manifest, each with the
    /// manifest's size and SHA-256.
    Ready(PathBuf),
    /// No folder holds every file of the manifest with its size and
    /// SHA-256.
    Absent,
}

/// A model file that is there but that Coral cannot read, for example for
/// lack of permission.
#[derive(Debug, thiserror::Error)]
#[error("cannot read {}: {source}", path.display())]
pub struct UnreadableModelFile {
    path: PathBuf,
    source: io::Error,
}

impl Manifest {
    /// Looks for this manifest's files in the folder `<model id>` of each
    /// of `models_dirs`, in order. One folder must hold every file. The
    /// lookup reads each file whole to check its SHA-256, the small
    /// tokenizer first.
    ///
    /// # Errors
    ///
    /// Returns [`UnreadableModelFile`] at the first file that is there but
    /// cannot be read. The lookup stops there, so a later folder does not
    /// hide a file that the operator put in place.
    pub fn find(&self, models_dirs: &[PathBuf]) -> Result<ModelState, UnreadableModelFile> {
        'folders: for models_dir in models_dirs {
            let dir = models_dir.join(self.model_id);
            for file in [self.tokenizer_file, self.model_file] {
                if !matches(&dir.join(file.name), &file)? {
                    continue 'folders;
                }
            }
            return Ok(ModelState::Ready(dir));
        }
        Ok(ModelState::Absent)
    }
}

/// Whether the file at `path` has `file`'s size and SHA-256. A file that is
/// not there does not match.
fn matches(path: &Path, file: &ModelFile) -> Result<bool, UnreadableModelFile> {
    let unreadable = |source| UnreadableModelFile {
        path: path.to_path_buf(),
        source,
    };
    let opened = match File::open(path) {
        Ok(opened) => opened,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(unreadable(error)),
    };
    let metadata = opened.metadata().map_err(unreadable)?;
    if !metadata.is_file() || metadata.len() != file.size {
        return Ok(false);
    }
    Ok(sha256(opened).map_err(unreadable)? == file.sha256)
}

/// The SHA-256 of `file`'s bytes, as lowercase hex.
fn sha256(file: File) -> io::Result<String> {
    let mut hasher = Sha256::new();
    let mut reader = BufReader::with_capacity(1 << 16, file);
    loop {
        let chunk = reader.fill_buf()?;
        if chunk.is_empty() {
            return Ok(hex(hasher.finalize().iter()));
        }
        hasher.update(chunk);
        let read = chunk.len();
        reader.consume(read);
    }
}
