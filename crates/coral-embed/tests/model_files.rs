//! Finding the manifest's files on disk, through the crate's public
//! interface, with a test manifest whose files are a few bytes.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]

use std::path::{Path, PathBuf};

use coral_embed::{MANIFEST, Manifest, ModelFile, ModelState};
use tempfile::TempDir;

const MODEL: &[u8] = b"model bytes";
const TOKENIZER: &[u8] = b"tokenizer bytes";

/// [`MANIFEST`] with files of a few bytes in place of the model's.
const TEST_MANIFEST: Manifest = Manifest {
    model_id: "test-model",
    model_file: ModelFile {
        name: "model.onnx",
        size: 11,
        sha256: "9cb7487000bc86ac36ce83c4acfabe8878552be99572a6770f65ab1d048a5c48",
    },
    tokenizer_file: ModelFile {
        name: "tokenizer.json",
        size: 15,
        sha256: "6ea0af464efe55bcdcd55fa91cde0e8b0e9647e59273110caee85c96de037652",
    },
    ..MANIFEST
};

/// What a test puts in a folder of model folders.
#[derive(Debug, Clone, Copy)]
enum Contents {
    /// Nothing.
    Nothing,
    /// Both files with the manifest's bytes.
    Both,
    /// The model with other bytes of the same size, so only its digest
    /// differs, and the tokenizer.
    OtherModelDigest,
    /// The model with one byte more, and the tokenizer.
    OtherModelSize,
    /// The model alone.
    ModelOnly,
    /// The tokenizer alone.
    TokenizerOnly,
    /// A folder named like the model file, and the tokenizer.
    ModelIsAFolder,
}

/// Puts `contents` in the test manifest's folder under `models_dir`.
fn fill(models_dir: &Path, contents: Contents) {
    let dir = models_dir.join(TEST_MANIFEST.model_id);
    let model = dir.join(TEST_MANIFEST.model_file.name);
    let tokenizer = dir.join(TEST_MANIFEST.tokenizer_file.name);
    let (model_bytes, tokenizer_bytes): (Option<&[u8]>, Option<&[u8]>) = match contents {
        Contents::Nothing => return,
        Contents::Both => (Some(MODEL), Some(TOKENIZER)),
        Contents::OtherModelDigest => (Some(b"model bytez"), Some(TOKENIZER)),
        Contents::OtherModelSize => (Some(b"model bytes!"), Some(TOKENIZER)),
        Contents::ModelOnly => (Some(MODEL), None),
        Contents::TokenizerOnly => (None, Some(TOKENIZER)),
        Contents::ModelIsAFolder => {
            std::fs::create_dir_all(&model).expect("create a folder at the model's path");
            (None, Some(TOKENIZER))
        }
    };
    std::fs::create_dir_all(&dir).expect("create the model folder");
    if let Some(bytes) = model_bytes {
        std::fs::write(&model, bytes).expect("write the model");
    }
    if let Some(bytes) = tokenizer_bytes {
        std::fs::write(&tokenizer, bytes).expect("write the tokenizer");
    }
}

/// Which folder of model folders a lookup finds the files in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Found {
    First,
    Second,
    Neither,
}

#[test]
fn the_first_folder_that_holds_every_file_with_its_digest_is_ready() {
    use Contents::{
        Both, ModelIsAFolder, ModelOnly, Nothing, OtherModelDigest, OtherModelSize, TokenizerOnly,
    };
    let cases = [
        (
            "only the second holds the files",
            Nothing,
            Both,
            Found::Second,
        ),
        ("both hold the files", Both, Both, Found::First),
        (
            "the first has another digest",
            OtherModelDigest,
            Both,
            Found::Second,
        ),
        (
            "only a file with another digest",
            Nothing,
            OtherModelDigest,
            Found::Neither,
        ),
        (
            "only a file with another size",
            Nothing,
            OtherModelSize,
            Found::Neither,
        ),
        ("only the model", Nothing, ModelOnly, Found::Neither),
        (
            "the files in different folders",
            ModelOnly,
            TokenizerOnly,
            Found::Neither,
        ),
        (
            "a folder in place of the model",
            Nothing,
            ModelIsAFolder,
            Found::Neither,
        ),
        ("no files", Nothing, Nothing, Found::Neither),
    ];
    for (case, first, second, expected) in cases {
        let temp = TempDir::new().unwrap();
        let first_dir = temp.path().join("first");
        let second_dir = temp.path().join("second");
        fill(&first_dir, first);
        fill(&second_dir, second);

        let state = TEST_MANIFEST
            .find(&[first_dir.clone(), second_dir.clone()])
            .expect("every file can be read");

        let expected = match expected {
            Found::First => ModelState::Ready(first_dir.join(TEST_MANIFEST.model_id)),
            Found::Second => ModelState::Ready(second_dir.join(TEST_MANIFEST.model_id)),
            Found::Neither => ModelState::Absent,
        };
        assert_eq!(state, expected, "{case}");
    }
}

/// A file that is there but cannot be read stops the lookup with its path,
/// also when a later folder holds the files: the operator learns why Coral
/// does not use the files they put in place.
#[cfg(unix)]
#[test]
fn a_file_that_cannot_be_read_stops_the_lookup_with_its_path() {
    let temp = TempDir::new().unwrap();
    let first_dir = temp.path().join("first");
    let second_dir = temp.path().join("second");
    fill(&first_dir, Contents::Both);
    fill(&second_dir, Contents::Both);
    // A link to itself: no user can open it, root included.
    let model = first_dir
        .join(TEST_MANIFEST.model_id)
        .join(TEST_MANIFEST.model_file.name);
    std::fs::remove_file(&model).unwrap();
    std::os::unix::fs::symlink(&model, &model).unwrap();

    let error = TEST_MANIFEST
        .find(&[first_dir, second_dir])
        .expect_err("the model file cannot be read");

    assert!(
        error.to_string().contains(&model.display().to_string()),
        "{error}"
    );
}

#[test]
fn coral_looks_in_the_system_folder_first_then_in_the_state_directory() {
    let expected: &[&str] = if cfg!(target_os = "linux") {
        &["/opt/coral/models", "/var/lib/coral/config/models"]
    } else {
        &["/var/lib/coral/config/models"]
    };

    assert_eq!(
        coral_embed::models_dirs(Path::new("/var/lib/coral/config")),
        expected.iter().map(PathBuf::from).collect::<Vec<_>>()
    );
}
