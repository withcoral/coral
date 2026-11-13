//! The lookup for the search model's files, and its result for the rest of
//! the Coral Instance.

use std::path::PathBuf;

use coral_embed::{MANIFEST, ModelState};
use tokio::sync::watch;

/// How the lookup for the search model's files ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchModelState {
    /// The lookup has not ended.
    Looking,
    /// This folder holds the files.
    Ready(PathBuf),
    /// No folder holds the files.
    Absent,
    /// A file is there, but Coral cannot read it. The lookup stops at it,
    /// so Coral uses no model until it starts with a file it can read.
    Unreadable(String),
}

/// The state of the files that Universal Search embeds text with. Search
/// ranks by words alone until the files are ready.
#[derive(Debug, Clone)]
pub struct SearchModel {
    state: watch::Receiver<SearchModelState>,
}

impl SearchModel {
    /// Starts a lookup for the manifest's files in `models_dirs`, in order.
    /// The lookup runs on a blocking thread, so boot does not wait for the
    /// digests.
    pub(crate) fn start_lookup(models_dirs: Vec<PathBuf>) -> Self {
        let (sender, state) = watch::channel(SearchModelState::Looking);
        tokio::task::spawn_blocking(move || {
            sender.send_replace(match MANIFEST.find(&models_dirs) {
                Ok(ModelState::Ready(dir)) => SearchModelState::Ready(dir),
                Ok(ModelState::Absent) => SearchModelState::Absent,
                Err(unreadable) => SearchModelState::Unreadable(unreadable.to_string()),
            });
        });
        Self { state }
    }

    /// Waits until the lookup ends and returns how it ended. Returns
    /// [`SearchModelState::Looking`] only when the runtime stops first.
    pub async fn lookup(&self) -> SearchModelState {
        let mut state = self.state.clone();
        state
            .wait_for(|state| *state != SearchModelState::Looking)
            .await
            .map_or(SearchModelState::Looking, |ended| ended.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{SearchModel, SearchModelState};

    #[tokio::test]
    async fn a_lookup_that_finds_no_files_ends_absent() {
        let temp = TempDir::new().unwrap();
        let model = SearchModel::start_lookup(vec![temp.path().join("models")]);

        let ended = tokio::time::timeout(Duration::from_secs(10), model.lookup())
            .await
            .expect("the lookup ends");

        assert_eq!(ended, SearchModelState::Absent);
    }

    /// The operator learns which file Coral cannot read.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_lookup_that_cannot_read_a_file_ends_with_its_path() {
        let temp = TempDir::new().unwrap();
        let dir = temp
            .path()
            .join("models")
            .join(coral_embed::MANIFEST.model_id);
        std::fs::create_dir_all(&dir).unwrap();
        // A link to itself: no user can open it, root included.
        let tokenizer = dir.join(coral_embed::MANIFEST.tokenizer_file.name);
        std::os::unix::fs::symlink(&tokenizer, &tokenizer).unwrap();
        let model = SearchModel::start_lookup(vec![temp.path().join("models")]);

        let ended = tokio::time::timeout(Duration::from_secs(10), model.lookup())
            .await
            .expect("the lookup ends");

        let SearchModelState::Unreadable(reason) = &ended else {
            panic!("expected an unreadable file: {ended:?}");
        };
        assert!(
            reason.contains(&tokenizer.display().to_string()),
            "{reason}"
        );
    }
}
