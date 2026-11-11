//! The real model against the bench harness's vectors.
//!
//! These tests load the 133 MB model, so they are ignored by default. Run
//! them with the model's files in one folder:
//!
//! ```text
//! CORAL_EMBED_MODEL_DIR=<folder with model.onnx and tokenizer.json> \
//!   cargo test -p coral-embed -- --ignored
//! ```
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]

use std::path::PathBuf;

use coral_embed::{Embed as _, MANIFEST, Model};
use serde::Deserialize;

/// The largest difference allowed in one component of a vector.
const TOLERANCE: f32 = 1e-5;

#[derive(Deserialize)]
struct Reference {
    documents: Vec<Case>,
    queries: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    text: String,
    vector: Vec<f32>,
}

fn reference() -> Reference {
    serde_json::from_str(include_str!("fixtures/reference-vectors.json"))
        .expect("the fixture is valid")
}

#[expect(
    clippy::disallowed_methods,
    reason = "the test reads where the developer put the model's files"
)]
fn model_dir() -> PathBuf {
    std::env::var_os("CORAL_EMBED_MODEL_DIR")
        .map(PathBuf::from)
        .expect("set CORAL_EMBED_MODEL_DIR to a folder with model.onnx and tokenizer.json")
}

fn assert_matches(label: &str, actual: &[f32], expected: &[f32]) {
    assert_eq!(actual.len(), MANIFEST.dimension, "{label}: dimension");
    assert_eq!(
        expected.len(),
        MANIFEST.dimension,
        "{label}: fixture dimension"
    );
    let norm = actual.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < TOLERANCE, "{label}: norm {norm}");
    let largest = actual
        .iter()
        .zip(expected)
        .map(|(a, e)| (a - e).abs())
        .fold(0.0_f32, f32::max);
    assert!(largest <= TOLERANCE, "{label}: differs by {largest}");
}

#[test]
#[ignore = "loads the model; set CORAL_EMBED_MODEL_DIR"]
fn documents_and_queries_match_the_harness() {
    let model = Model::load(&model_dir()).expect("the model loads");
    let reference = reference();
    for (index, case) in reference.documents.iter().enumerate() {
        let vector = model
            .embed_document(&case.text)
            .expect("the document embeds");
        assert_matches(&format!("document {index}"), &vector, &case.vector);
    }
    for (index, case) in reference.queries.iter().enumerate() {
        let vector = model.embed_query(&case.text).expect("the query embeds");
        assert_matches(&format!("query {index}"), &vector, &case.vector);
    }
}

#[test]
#[ignore = "loads the model; set CORAL_EMBED_MODEL_DIR"]
fn two_models_load_in_one_process() {
    let first = Model::load(&model_dir()).expect("the first model loads");
    let second = Model::load(&model_dir()).expect("the second model loads");
    let reference = reference();
    let text = reference.queries.first().expect("the fixture has a query");
    for model in [&first, &second] {
        let vector = model.embed_query(&text.text).expect("the query embeds");
        assert_matches("query", &vector, &text.vector);
    }
}
