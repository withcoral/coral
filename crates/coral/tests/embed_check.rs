#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test: only the embed-check process and its output are under test."
)]

use assert_cmd::Command;
use coral_embed::MANIFEST;

/// `embed-check` prints numbers under the manifest's model id, so it loads
/// only files with the manifest's size and SHA-256. The tokenizer file is
/// checked first, so these cases never read a 133 MB model file.
#[test]
fn embed_check_refuses_files_that_differ_from_the_manifest() {
    let tokenizer = MANIFEST.tokenizer_file;
    let wrong_size = vec![0_u8; 3];
    let right_size = vec![0_u8; usize::try_from(tokenizer.size).unwrap()];
    for (contents, expected) in [
        (
            &wrong_size,
            format!("has 3 bytes, expected {} bytes", tokenizer.size),
        ),
        (&right_size, format!("expected {}", tokenizer.sha256)),
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(tokenizer.name), contents).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_coral"))
            .args(["embed-check", "--model-dir"])
            .arg(dir.path())
            .output()
            .unwrap();

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{stderr}");
        assert!(stderr.contains(tokenizer.name), "{stderr}");
        assert!(stderr.contains(&expected), "{expected}: {stderr}");
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains("load_ms"),
            "the model loaded"
        );
    }
}
