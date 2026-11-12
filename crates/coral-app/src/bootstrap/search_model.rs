//! The lookup for the search model's files, and its result for the rest of
//! the Coral Instance.

use std::path::PathBuf;

use coral_embed::{MANIFEST, ModelState};
use tokio::sync::watch;

/// The state of the files that Universal Search embeds text with. The
/// state is absent until a lookup finds the files. Search ranks by words
/// alone while the state is absent.
#[derive(Debug, Clone)]
pub struct SearchModel {
    state: watch::Receiver<ModelState>,
}

impl SearchModel {
    /// Starts a lookup for the manifest's files in `models_dirs`, in order.
    /// The lookup runs on a blocking thread, so boot does not wait for the
    /// digests.
    pub(crate) fn start_lookup(models_dirs: Vec<PathBuf>) -> Self {
        let (sender, state) = watch::channel(ModelState::Absent);
        tokio::task::spawn_blocking(move || {
            sender.send_replace(MANIFEST.find(&models_dirs));
        });
        Self { state }
    }

    /// Waits until the files are ready and returns their folder. Returns
    /// `None` when the lookup ends without them.
    pub async fn ready(&self) -> Option<PathBuf> {
        let mut state = self.state.clone();
        let ready = state
            .wait_for(|state| matches!(state, ModelState::Ready(_)))
            .await
            .ok()?;
        let ModelState::Ready(dir) = &*ready else {
            return None;
        };
        Some(dir.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::SearchModel;

    #[tokio::test]
    async fn a_lookup_that_finds_no_files_ends_absent() {
        let temp = TempDir::new().unwrap();
        let model = SearchModel::start_lookup(vec![temp.path().join("models")]);

        let ready = tokio::time::timeout(Duration::from_secs(10), model.ready())
            .await
            .expect("the lookup ends");

        assert_eq!(ready, None);
    }
}
