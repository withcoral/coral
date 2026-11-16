//! The server image holds the manifest's model. The Dockerfile cannot read
//! the manifest. It names the values again, and this test keeps them equal.
//! `make docker-test` proves that Coral finds the image's files.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]

use std::path::Path;

use coral_embed::{MANIFEST, SYSTEM_MODELS_DIR};

#[test]
fn the_server_image_fetches_the_manifests_files_into_the_system_folder() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docker/Dockerfile");
    let dockerfile = std::fs::read_to_string(&path).expect("read docker/Dockerfile");

    let mut expected = vec![
        MANIFEST.download_url.to_owned(),
        format!("{SYSTEM_MODELS_DIR}/{}", MANIFEST.model_id),
    ];
    for file in [MANIFEST.model_file, MANIFEST.tokenizer_file] {
        // A `sha256sum --check` line.
        expected.push(format!("{}  {}", file.sha256, file.name));
    }
    let missing = expected
        .iter()
        .filter(|text| !dockerfile.contains(text.as_str()))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "docker/Dockerfile does not name {missing:?}"
    );
}
