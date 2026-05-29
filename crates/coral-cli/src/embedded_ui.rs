//! Embedded Coral UI assets compiled into the binary.

use std::borrow::Cow;

use coral_app::{StaticAsset, StaticAssetsProvider};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../../ui/dist/"]
#[allow_missing = true]
struct EmbeddedUiFiles;

pub(crate) struct EmbeddedUi;

impl StaticAssetsProvider for EmbeddedUi {
    fn get(&self, path: &str) -> Option<StaticAsset> {
        let lookup = if path.is_empty() { "index.html" } else { path };
        let file = EmbeddedUiFiles::get(lookup)?;
        let content_type = Cow::Owned(file.metadata.mimetype().to_string());
        Some(StaticAsset {
            bytes: file.data,
            content_type,
        })
    }
}
