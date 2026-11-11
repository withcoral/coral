//! The model manifest compiled into the binary, and the Embedder revision
//! derived from it.

use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

/// The model Universal Search embeds with, as ADR 0033 picks it:
/// `BAAI/bge-small-en-v1.5`, the author's fp32 ONNX file at source revision
/// `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`, MIT.
///
/// There is no operator override. A model change is a change to this value,
/// which changes the [`EmbedderRevision`] and re-embeds every Search Entity.
pub const MANIFEST: Manifest = Manifest {
    model_id: "bge-small-en-v1.5",
    download_url: "https://github.com/withcoral/coral-releases/releases/download/model-bge-small-en-v1.5-5c38ec7",
    model_file: ModelFile {
        name: "model.onnx",
        size: 133_093_490,
        sha256: "828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35",
    },
    tokenizer_file: ModelFile {
        name: "tokenizer.json",
        size: 711_396,
        sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
    },
    dimension: 384,
    max_tokens: 512,
    pooling: Pooling::Cls,
    query_prefix: "Represent this sentence for searching relevant passages: ",
    document_prefix: "",
    index_text: IndexText::PathTitleDescriptionParameterNames,
    query_text: QueryText::FrozenQuery,
    merge: Merge {
        k: 60,
        lexical_weight: 1,
        vector_weight: 1,
    },
    strong_count: 5,
};

/// One embedding model and how Universal Search uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Manifest {
    /// The model's name in Coral. The model's files live in a folder of this
    /// name, so it holds no path separator.
    pub model_id: &'static str,
    /// The release that holds the model's files. A file downloads from this
    /// URL, a slash and the file's name.
    pub download_url: &'static str,
    /// The ONNX graph.
    pub model_file: ModelFile,
    /// The `tokenizers` JSON file.
    pub tokenizer_file: ModelFile,
    /// The length of every vector.
    pub dimension: usize,
    /// The tokens the model reads from a text, special tokens included. The
    /// tokenizer cuts the rest of a longer text.
    pub max_tokens: usize,
    /// How one vector comes from the model's per-token output.
    pub pooling: Pooling,
    /// Text that goes before a query.
    pub query_prefix: &'static str,
    /// Text that goes before a Search Entity's index text.
    pub document_prefix: &'static str,
    /// What text of a Search Entity the Embedder embeds.
    pub index_text: IndexText,
    /// What text of a search request the engine embeds.
    pub query_text: QueryText,
    /// How a retriever merges its vector list with its lexical list.
    pub merge: Merge,
    /// An entry is strong when its rank in the vector list is this number
    /// or lower. It is a constant: it does not follow the request's limit.
    pub strong_count: usize,
}

/// One file of the model, as the release holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelFile {
    /// The file's name in the release and in the model's folder.
    pub name: &'static str,
    /// The file's size in bytes.
    pub size: u64,
    /// The file's SHA-256, lowercase hex.
    pub sha256: &'static str,
}

/// How one vector comes from the model's per-token output. Every pooling
/// ends with L2 normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pooling {
    /// The output of the first token, `[CLS]`.
    Cls,
}

/// What text of a Search Entity the Embedder embeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexText {
    /// For a Provider Tool, the Call Path, title, description and parameter
    /// names. For a Saved Function, the Call Path, description and input
    /// names. The bench names it `path_title_desc_params`.
    PathTitleDescriptionParameterNames,
}

/// What text of a search request the engine embeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryText {
    /// The frozen query alone.
    FrozenQuery,
}

/// Reciprocal rank merge of a retriever's lexical and vector lists: an
/// entry scores `weight / (k + rank)` from each list it is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Merge {
    /// The rank constant.
    pub k: u32,
    /// The lexical list's weight.
    pub lexical_weight: u32,
    /// The vector list's weight.
    pub vector_weight: u32,
}

/// A digest of the manifest fields that change stored vectors. A stored
/// vector with another revision is stale. It is separate from the indexer's
/// revision, so a model change re-embeds without re-indexing words.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmbedderRevision(String);

impl EmbedderRevision {
    /// The revision as 16 lowercase hex characters.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EmbedderRevision {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Manifest {
    /// The [`EmbedderRevision`] of this manifest.
    #[must_use]
    pub fn embedder_revision(&self) -> EmbedderRevision {
        // Every field is named, so a new field does not compile until it is
        // put in the digest or left out on purpose.
        let Self {
            model_id,
            download_url: _,
            model_file,
            tokenizer_file,
            dimension,
            max_tokens,
            pooling,
            query_prefix: _,
            document_prefix,
            index_text,
            query_text: _,
            merge: _,
            strong_count: _,
        } = self;
        let mut digest = Sha256::new();
        for (label, value) in [
            ("model_id", model_id.as_bytes()),
            ("model_file.sha256", model_file.sha256.as_bytes()),
            ("tokenizer_file.sha256", tokenizer_file.sha256.as_bytes()),
            ("dimension", dimension.to_string().as_bytes()),
            ("max_tokens", max_tokens.to_string().as_bytes()),
            ("pooling", pooling.key().as_bytes()),
            ("document_prefix", document_prefix.as_bytes()),
            ("index_text", index_text.key().as_bytes()),
        ] {
            // Length prefixes keep two different field lists from giving
            // the same bytes.
            for part in [label.as_bytes(), value] {
                digest.update((part.len() as u64).to_le_bytes());
                digest.update(part);
            }
        }
        let mut hex = String::with_capacity(16);
        for byte in digest.finalize().iter().take(8) {
            write!(hex, "{byte:02x}").expect("writing to a String does not fail");
        }
        EmbedderRevision(hex)
    }
}

impl Pooling {
    /// A stable name for the Embedder revision.
    const fn key(self) -> &'static str {
        match self {
            Self::Cls => "cls",
        }
    }
}

impl IndexText {
    /// A stable name for the Embedder revision: the bench's name.
    const fn key(self) -> &'static str {
        match self {
            Self::PathTitleDescriptionParameterNames => "path_title_desc_params",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The revision of the shipped manifest. A change here re-embeds every
    /// Search Entity of every Workspace, so it must come from a change to
    /// the model, never from a refactor.
    const SHIPPED_REVISION: &str = "a7566a6108994f30";

    #[test]
    fn shipped_manifest_has_a_pinned_revision() {
        assert_eq!(MANIFEST.embedder_revision().as_str(), SHIPPED_REVISION);
    }

    #[test]
    fn revision_changes_exactly_when_stored_vectors_change() {
        const OTHER_DIGEST: &str =
            "0000000000000000000000000000000000000000000000000000000000000000";
        type Change = fn(&mut Manifest);
        let cases: [(&str, Change, bool); 16] = [
            ("model id", |m| m.model_id = "other-model", true),
            (
                "model file digest",
                |m| m.model_file.sha256 = OTHER_DIGEST,
                true,
            ),
            (
                "tokenizer file digest",
                |m| m.tokenizer_file.sha256 = OTHER_DIGEST,
                true,
            ),
            ("dimension", |m| m.dimension = 768, true),
            ("maximum tokens", |m| m.max_tokens = 256, true),
            ("document prefix", |m| m.document_prefix = "passage: ", true),
            (
                "download URL",
                |m| m.download_url = "https://example.com/models",
                false,
            ),
            (
                "model file name",
                |m| m.model_file.name = "renamed.bin",
                false,
            ),
            (
                "tokenizer file name",
                |m| m.tokenizer_file.name = "renamed.json",
                false,
            ),
            ("model file size", |m| m.model_file.size += 1, false),
            ("query prefix", |m| m.query_prefix = "query: ", false),
            ("merge k", |m| m.merge.k = 30, false),
            ("lexical weight", |m| m.merge.lexical_weight = 2, false),
            ("vector weight", |m| m.merge.vector_weight = 2, false),
            ("strong count", |m| m.strong_count = 10, false),
            ("nothing", |_| {}, false),
        ];
        let shipped = MANIFEST.embedder_revision();
        for (field, change, changes_vectors) in cases {
            let mut manifest = MANIFEST;
            change(&mut manifest);
            assert_eq!(
                manifest.embedder_revision() != shipped,
                changes_vectors,
                "{field}"
            );
        }
    }
}
