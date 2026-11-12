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
    dirs.push(state_models_dir(state_dir));
    dirs
}

/// The folder of model folders in the state directory `state_dir`. Coral
/// downloads a model's files into it.
#[must_use]
pub fn state_models_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("models")
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

impl Manifest {
    /// Every file of the model. A folder that holds all of them, each with
    /// its size and SHA-256, holds the model.
    #[must_use]
    pub const fn files(&self) -> [ModelFile; 2] {
        [self.tokenizer_file, self.model_file]
    }

    /// Looks for this manifest's files in the folder `<model id>` of each
    /// of `models_dirs`, in order. One folder must hold every file. The
    /// lookup reads each file whole to check its SHA-256.
    #[must_use]
    pub fn find(&self, models_dirs: &[PathBuf]) -> ModelState {
        for models_dir in models_dirs {
            let dir = models_dir.join(self.model_id);
            if self
                .files()
                .iter()
                .all(|file| file.is_at(&dir.join(file.name)))
            {
                return ModelState::Ready(dir);
            }
        }
        ModelState::Absent
    }
}

impl ModelFile {
    /// Whether the file at `path` has this file's size and SHA-256. A file
    /// that cannot be read does not match. It reads the file whole.
    #[must_use]
    pub fn is_at(&self, path: &Path) -> bool {
        let Ok(opened) = File::open(path) else {
            return false;
        };
        match opened.metadata() {
            Ok(metadata) if metadata.is_file() && metadata.len() == self.size => {}
            _ => return false,
        }
        sha256(opened).is_ok_and(|digest| digest == self.sha256)
    }
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
