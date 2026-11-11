//! The model in an ONNX Runtime session, behind the text-to-vector interface.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use ort::environment::Environment;
use ort::session::Session;
use ort::session::builder::{AutoDevicePolicy, GraphOptimizationLevel, SessionBuilder};
use ort::value::Tensor;
use tokenizers::{Tokenizer, TruncationDirection, TruncationParams, TruncationStrategy};

use crate::cpu::{CpuTooOld, check_cpu};
use crate::manifest::{MANIFEST, Pooling};

/// Text in, vector out. Search code takes this interface, so its tests can
/// give fixed vectors and load no model.
///
/// A vector has [`MANIFEST`]'s dimension and an L2 norm of 1.
pub trait Embed: Send + Sync {
    /// The vector of a search request's query text. The manifest's query
    /// prefix goes before the text.
    ///
    /// # Errors
    ///
    /// [`EmbedError`] when the text cannot become a vector.
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError>;

    /// The vector of a Search Entity's index text. The manifest's document
    /// prefix goes before the text.
    ///
    /// # Errors
    ///
    /// [`EmbedError`] when the text cannot become a vector.
    fn embed_document(&self, text: &str) -> Result<Vec<f32>, EmbedError>;
}

/// A text did not become a vector.
#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    /// The tokenizer refused the text.
    #[error("tokenize the text")]
    Tokenize(#[source] tokenizers::Error),
    /// ONNX Runtime failed to run the model.
    #[error("run the model")]
    Inference(#[source] ort::Error),
    /// The model's output is not a vector of the manifest's dimension.
    #[error("the model gave an unexpected output: {0}")]
    Output(String),
}

/// The model did not load.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The CPU is below the floor, so no file was read.
    #[error(transparent)]
    CpuTooOld(#[from] CpuTooOld),
    /// The tokenizer file did not load.
    #[error("load the tokenizer {}", .path.display())]
    Tokenizer {
        /// The tokenizer file.
        path: PathBuf,
        /// What failed.
        #[source]
        source: tokenizers::Error,
    },
    /// ONNX Runtime did not load the model file.
    #[error("load the model {}", .path.display())]
    Session {
        /// The model file.
        path: PathBuf,
        /// What failed.
        #[source]
        source: ort::Error,
    },
}

/// The manifest's model, loaded from its files.
///
/// Calls take turns on one session, each on the calling thread: the session
/// has one intra-op thread, so the caller's thread priority applies. Give
/// work that must not wait for another caller its own `Model`.
pub struct Model {
    inference: Mutex<Inference>,
}

struct Inference {
    tokenizer: Tokenizer,
    session: Session,
    takes_token_type_ids: bool,
}

impl std::fmt::Debug for Model {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Model").finish_non_exhaustive()
    }
}

impl Model {
    /// Loads the manifest's files from `dir`, where they have the
    /// manifest's names. It does not check their digests; the caller that
    /// found the files does.
    ///
    /// # Errors
    ///
    /// [`LoadError::CpuTooOld`] below the CPU floor, before any file is
    /// read; otherwise the file that did not load.
    pub fn load(dir: &Path) -> Result<Self, LoadError> {
        check_cpu()?;
        let path = dir.join(MANIFEST.tokenizer_file.name);
        let tokenizer = load_tokenizer(&path).map_err(|source| LoadError::Tokenizer {
            path: path.clone(),
            source,
        })?;
        let path = dir.join(MANIFEST.model_file.name);
        let session = session_builder()
            .and_then(|mut builder| builder.commit_from_file(&path))
            .map_err(|source| LoadError::Session {
                path: path.clone(),
                source,
            })?;
        let takes_token_type_ids = session
            .inputs()
            .iter()
            .any(|input| input.name() == "token_type_ids");
        Ok(Self {
            inference: Mutex::new(Inference {
                tokenizer,
                session,
                takes_token_type_ids,
            }),
        })
    }

    fn embed(&self, prefix: &str, text: &str) -> Result<Vec<f32>, EmbedError> {
        // A panic inside a call leaves no state behind in the tokenizer or
        // the session, so a poisoned lock is still safe to use.
        let mut inference = self
            .inference
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        inference.embed(&format!("{prefix}{text}"))
    }
}

impl Embed for Model {
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        self.embed(MANIFEST.query_prefix, text)
    }

    fn embed_document(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        self.embed(MANIFEST.document_prefix, text)
    }
}

impl Inference {
    /// One text, one run: special tokens added, cut at the manifest's
    /// maximum tokens on the right, pooled and normalized.
    fn embed(&mut self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(EmbedError::Tokenize)?;
        let ids: Vec<i64> = encoding.get_ids().iter().map(|&id| i64::from(id)).collect();
        let mask: Vec<i64> = encoding
            .get_attention_mask()
            .iter()
            .map(|&bit| i64::from(bit))
            .collect();
        let tokens = ids.len();
        let shape = [1_usize, tokens];
        let ids = Tensor::from_array((shape, ids)).map_err(EmbedError::Inference)?;
        let mask = Tensor::from_array((shape, mask)).map_err(EmbedError::Inference)?;
        let outputs = if self.takes_token_type_ids {
            let types =
                Tensor::from_array((shape, vec![0_i64; tokens])).map_err(EmbedError::Inference)?;
            self.session.run(ort::inputs![
                "input_ids" => ids,
                "attention_mask" => mask,
                "token_type_ids" => types,
            ])
        } else {
            self.session.run(ort::inputs![
                "input_ids" => ids,
                "attention_mask" => mask,
            ])
        }
        .map_err(EmbedError::Inference)?;
        let output = outputs
            .values()
            .next()
            .ok_or_else(|| EmbedError::Output("no output".to_owned()))?;
        let (shape, data) = output
            .try_extract_tensor::<f32>()
            .map_err(EmbedError::Inference)?;
        let dimension = MANIFEST.dimension;
        let width = match &**shape {
            [_, _, width] => usize::try_from(*width).ok(),
            _ => None,
        };
        if width != Some(dimension) {
            return Err(EmbedError::Output(format!(
                "shape {shape:?}, expected [1, tokens, {dimension}]"
            )));
        }
        let mut vector = match MANIFEST.pooling {
            Pooling::Cls => data
                .get(..dimension)
                .ok_or_else(|| EmbedError::Output(format!("{} values", data.len())))?
                .to_vec(),
        };
        let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if !norm.is_normal() {
            return Err(EmbedError::Output(format!("vector norm {norm}")));
        }
        for x in &mut vector {
            *x /= norm;
        }
        Ok(vector)
    }
}

fn load_tokenizer(path: &Path) -> Result<Tokenizer, tokenizers::Error> {
    let mut tokenizer = Tokenizer::from_file(path)?;
    tokenizer.with_truncation(Some(TruncationParams {
        max_length: MANIFEST.max_tokens,
        strategy: TruncationStrategy::LongestFirst,
        stride: 0,
        direction: TruncationDirection::Right,
    }))?;
    tokenizer.with_padding(None);
    Ok(tokenizer)
}

/// Every session starts here. `PreferCPU` keeps ONNX Runtime from placing
/// part of the graph on another device: its default puts nodes on `CoreML` on
/// macOS, four times slower with about seven times the memory (#2867). One
/// intra-op thread runs the model on the caller's thread.
fn session_builder() -> Result<SessionBuilder, ort::Error> {
    Ok(Session::builder(&environment()?)?
        .with_auto_device(AutoDevicePolicy::PreferCPU)?
        .with_optimization_level(GraphOptimizationLevel::All)?
        .with_intra_threads(1)?)
}

/// ONNX Runtime allows one environment per process. `ort` refuses a second
/// one, even after the first is dropped. So every session shares this one,
/// which lives as long as the process.
fn environment() -> Result<Environment, ort::Error> {
    static ENVIRONMENT: Mutex<Option<Environment>> = Mutex::new(None);
    let mut slot = ENVIRONMENT.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(environment) = slot.as_ref() {
        return Ok(environment.clone());
    }
    let environment = ort::init().with_name("coral-embed").build()?;
    *slot = Some(environment.clone());
    Ok(environment)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[expect(
        clippy::disallowed_methods,
        reason = "the test reads where the developer put the model's files"
    )]
    fn model_dir() -> PathBuf {
        std::env::var_os("CORAL_EMBED_MODEL_DIR")
            .map(PathBuf::from)
            .expect("set CORAL_EMBED_MODEL_DIR to a folder with model.onnx and tokenizer.json")
    }

    /// Without `PreferCPU`, ONNX Runtime puts part of the graph on `CoreML` on
    /// macOS. Elsewhere no other device is registered, so this test can fail
    /// only on macOS.
    #[test]
    #[ignore = "loads the model; set CORAL_EMBED_MODEL_DIR"]
    fn every_node_runs_on_the_cpu() {
        let dir = model_dir();
        let profile = tempfile::tempdir().unwrap();
        let session = session_builder()
            .unwrap()
            .with_profiling(profile.path().join("profile"))
            .unwrap()
            .commit_from_file(dir.join(MANIFEST.model_file.name))
            .unwrap();
        let mut inference = Inference {
            tokenizer: load_tokenizer(&dir.join(MANIFEST.tokenizer_file.name)).unwrap(),
            session,
            takes_token_type_ids: true,
        };
        inference
            .embed("List the open pull requests of a repository.")
            .unwrap();
        let file = inference.session.end_profiling().unwrap();
        let events: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
        let providers: std::collections::BTreeSet<&str> = events
            .iter()
            .filter(|event| event.get("cat").and_then(serde_json::Value::as_str) == Some("Node"))
            .filter_map(|event| event.pointer("/args/provider")?.as_str())
            .collect();
        assert_eq!(
            providers,
            std::collections::BTreeSet::from(["CPUExecutionProvider"])
        );
    }
}
