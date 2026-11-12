//! The delivery of the search model's files, and its result for the rest of
//! the Coral Instance.
//!
//! Coral looks for the manifest's files in each folder of model folders, in
//! order. When no folder holds them, it downloads them from the model
//! release through the outbound client, into the state directory. A file
//! gets its final name only after its size and SHA-256 match. A failed
//! attempt waits, then tries again, while the process runs. Once the files
//! are ready, the other model folders in the state directory go. The ADR
//! "Universal Search adds a vector list from a bundled CPU model" has the
//! reasons.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coral_embed::{Embed, LoadError, Manifest, Model, ModelFile, ModelState};
use coral_telemetry::safe;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use url::Url;

/// Where the search model's files are. Search ranks by words alone until
/// they are ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchModelState {
    /// Coral looks for the files on disk.
    Looking,
    /// No folder holds the files. Coral downloads them and retries until
    /// they land.
    Downloading,
    /// This folder holds every file of the manifest, each with the
    /// manifest's size and SHA-256.
    Ready(PathBuf),
}

/// The state of the files that Universal Search embeds text with.
#[derive(Debug, Clone)]
pub struct SearchModel {
    state: watch::Receiver<SearchModelState>,
}

/// What [`SearchModel::start`] looks for, where, and how it downloads.
pub(crate) struct Delivery {
    pub(crate) manifest: Manifest,
    /// The folders of model folders to look in, in order.
    pub(crate) models_dirs: Vec<PathBuf>,
    /// The folder of model folders that a download goes to. Once the files
    /// are ready, Coral removes every other model folder from it.
    pub(crate) download_dir: PathBuf,
    /// The client every download request goes through, with the operator's
    /// outbound destination policy.
    pub(crate) client: coral_outbound::Client,
    pub(crate) timing: DownloadTiming,
}

/// How long a download waits, and for what.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DownloadTiming {
    /// The wait after the first failed attempt. Each later wait doubles.
    pub(crate) first_retry: Duration,
    /// The longest wait between attempts.
    pub(crate) max_retry: Duration,
    /// The shortest wait after an attempt that got a whole file with
    /// another size or SHA-256. Such a file comes back on the next attempt
    /// too, so a short wait downloads the model again and again.
    pub(crate) wrong_file_retry: Duration,
    /// An attempt fails when the server sends nothing for this long.
    pub(crate) stall: Duration,
}

/// How long a download request may wait to connect.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest one file's transfer may take. The stall bound ends a
/// transfer that stops long before this.
const TRANSFER_LIMIT: Duration = Duration::from_hours(6);

/// GitHub sends a release download to its asset host with one redirect.
const MAX_REDIRECTS: usize = 5;

impl Delivery {
    /// The delivery of [`coral_embed::MANIFEST`] for a Coral process whose
    /// state directory is `state_dir`.
    pub(crate) fn coral(state_dir: &Path, client: coral_outbound::Client) -> Self {
        Self {
            manifest: coral_embed::MANIFEST,
            models_dirs: coral_embed::models_dirs(state_dir),
            download_dir: coral_embed::state_models_dir(state_dir),
            client,
            timing: DownloadTiming {
                first_retry: Duration::from_secs(10),
                max_retry: Duration::from_mins(15),
                wrong_file_retry: Duration::from_hours(6),
                stall: Duration::from_mins(1),
            },
        }
    }
}

impl SearchModel {
    /// Starts the delivery. Abort the handle to stop it.
    pub(crate) fn start(delivery: Delivery) -> (Self, JoinHandle<()>) {
        let (sender, state) = watch::channel(SearchModelState::Looking);
        (Self { state }, tokio::spawn(deliver(delivery, sender)))
    }

    /// Where the files are now.
    #[must_use]
    pub fn state(&self) -> SearchModelState {
        self.state.borrow().clone()
    }

    /// Waits until the lookup on disk ends and returns the state after it.
    /// A download that ends at once can be missed: then the state is
    /// already ready. Returns `None` when the delivery stops first.
    pub async fn wait_for_lookup(&self) -> Option<SearchModelState> {
        let mut state = self.state.clone();
        let after_lookup = state
            .wait_for(|state| *state != SearchModelState::Looking)
            .await
            .ok()?;
        Some(after_lookup.clone())
    }

    /// Waits until the files are ready and returns their folder. Returns
    /// `None` when the delivery stops without them.
    pub async fn ready(&self) -> Option<PathBuf> {
        let mut state = self.state.clone();
        let ready = state
            .wait_for(|state| matches!(state, SearchModelState::Ready(_)))
            .await
            .ok()?;
        let SearchModelState::Ready(dir) = &*ready else {
            return None;
        };
        Some(dir.clone())
    }

    /// Loads the model into `slot` once its files are ready, on a blocking
    /// thread. The slot stays empty when the files never arrive, when the
    /// CPU is below the floor, or when the model does not load: search then
    /// matches by words alone.
    pub(crate) fn load_into(&self, slot: watch::Sender<Option<Arc<dyn Embed>>>) -> JoinHandle<()> {
        let files = self.clone();
        tokio::spawn(async move {
            let Some(dir) = files.ready().await else {
                return;
            };
            match tokio::task::spawn_blocking(move || Model::load(&dir)).await {
                Ok(Ok(model)) => {
                    let model: Arc<dyn Embed> = Arc::new(model);
                    slot.send_replace(Some(model));
                }
                Ok(Err(LoadError::CpuTooOld(_))) => tracing::info!(
                    "this CPU is below the floor for search by meaning; search matches by words only"
                ),
                Ok(Err(error)) => tracing::error!(
                    error.type = safe::kind(match error {
                        LoadError::Tokenizer { .. } => "tokenizer_load_failed",
                        _ => "model_load_failed",
                    }),
                    "failed to load the search model; search matches by words only"
                ),
                Err(_) => tracing::error!(
                    error.type = safe::kind("model_load_panicked"),
                    "loading the search model panicked; search matches by words only"
                ),
            }
        })
    }
}

/// Finds the files, or downloads them until they land, and reports where
/// they are.
async fn deliver(delivery: Delivery, state: watch::Sender<SearchModelState>) {
    let Delivery {
        manifest,
        models_dirs,
        download_dir,
        client,
        timing,
    } = delivery;
    // The lookup ends without a result only when the runtime stops. Then
    // the state's sender drops, and `ready` returns `None`.
    let Ok(found) = tokio::task::spawn_blocking(move || manifest.find(&models_dirs)).await else {
        return;
    };
    let dir = match found {
        ModelState::Ready(dir) => dir,
        ModelState::Absent => {
            state.send_replace(SearchModelState::Downloading);
            let dir = download_dir.join(manifest.model_id);
            let mut wait = timing.first_retry;
            while let Err(error) = download(&client, &manifest, &dir, timing.stall).await {
                tracing::info!(
                    error.type = safe::kind(error.code()),
                    "search model download failed; Coral retries"
                );
                let pause = match error {
                    DownloadError::WrongFile => wait.max(timing.wrong_file_retry),
                    _ => wait,
                };
                tokio::time::sleep(pause).await;
                wait = wait.saturating_mul(2).min(timing.max_retry);
            }
            dir
        }
    };
    let model_id = manifest.model_id;
    // The removal ends before the state turns ready, so a reader of the
    // ready state never sees the other folders.
    if tokio::task::spawn_blocking(move || remove_other_models(&download_dir, model_id))
        .await
        .is_err()
    {
        tracing::warn!(
            error.type = safe::kind("join"),
            "the removal of old search model folders stopped; the next start retries"
        );
    }
    state.send_replace(SearchModelState::Ready(dir));
}

/// Removes every folder in `download_dir` but the folder of `model_id`.
/// The folders of an earlier manifest's model are of no use to this
/// binary. A failure leaves the folder for the next start.
fn remove_other_models(download_dir: &Path, model_id: &str) {
    let entries = match std::fs::read_dir(download_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(_error) => {
            tracing::warn!(
                error.type = safe::kind("io"),
                "the search model folder could not be read; old models stay"
            );
            return;
        }
    };
    for entry in entries.flatten() {
        // `file_type` does not follow a symbolic link, so a link to a
        // folder elsewhere stays, with its target.
        let is_folder = entry.file_type().is_ok_and(|kind| kind.is_dir());
        if is_folder
            && entry.file_name() != model_id
            && std::fs::remove_dir_all(entry.path()).is_err()
        {
            tracing::warn!(
                error.type = safe::kind("io"),
                "an old search model folder could not be removed; the next start retries"
            );
        }
    }
}

/// Why one download attempt did not put the files in place.
#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error("the download URL is not a URL")]
    InvalidUrl,
    #[error("the outbound destination policy rejects the download")]
    DestinationRejected,
    #[error("the request failed")]
    Http(reqwest::Error),
    #[error("the server sent nothing for too long")]
    Stalled,
    #[error("a redirect has no usable location, or there are too many")]
    BadRedirect,
    #[error("the file does not have the manifest's size and SHA-256")]
    WrongFile,
    #[error("a file could not be written")]
    Io(#[from] std::io::Error),
    #[error("the file check stopped")]
    Check(#[from] tokio::task::JoinError),
}

impl DownloadError {
    fn from_fetch(error: reqwest::Error) -> Self {
        match coral_outbound::FetchError::from(error) {
            coral_outbound::FetchError::DestinationRejected => Self::DestinationRejected,
            coral_outbound::FetchError::Http(error) => Self::Http(error),
        }
    }

    /// The failure's stable code.
    const fn code(&self) -> &'static str {
        match self {
            Self::InvalidUrl => "invalid_url",
            Self::DestinationRejected => "destination_rejected",
            Self::Http(_) => "http",
            Self::Stalled => "stalled",
            Self::BadRedirect => "bad_redirect",
            Self::WrongFile => "wrong_file",
            Self::Io(_) => "io",
            Self::Check(_) => "check",
        }
    }
}

/// Downloads each file of `manifest` that `dir` does not hold yet.
async fn download(
    client: &coral_outbound::Client,
    manifest: &Manifest,
    dir: &Path,
    stall: Duration,
) -> Result<(), DownloadError> {
    tokio::fs::create_dir_all(dir).await?;
    for file in manifest.files() {
        let target = dir.join(file.name);
        if is_at(file, target.clone()).await? {
            continue;
        }
        let partial = dir.join(format!("{}.part", file.name));
        let checked = async {
            fetch(client, manifest, file, &partial, stall).await?;
            if is_at(file, partial.clone()).await? {
                Ok(())
            } else {
                Err(DownloadError::WrongFile)
            }
        }
        .await;
        if let Err(error) = checked {
            // A partial file is of no use. One that stays is written again
            // from the start by the next attempt.
            match tokio::fs::remove_file(&partial).await {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    tracing::debug!(
                        error.type = safe::kind("io"),
                        "a partial search model file stays until the next attempt"
                    );
                }
                _ => {}
            }
            return Err(error);
        }
        tokio::fs::rename(&partial, &target).await?;
    }
    Ok(())
}

/// Whether the file at `path` has `file`'s size and SHA-256. It reads the
/// file whole, on a blocking thread.
async fn is_at(file: ModelFile, path: PathBuf) -> Result<bool, DownloadError> {
    Ok(tokio::task::spawn_blocking(move || file.is_at(&path)).await?)
}

/// Writes the release's `file` to `partial`. Each redirect goes through
/// `client` again, so the destination policy checks every host.
async fn fetch(
    client: &coral_outbound::Client,
    manifest: &Manifest,
    file: ModelFile,
    partial: &Path,
    stall: Duration,
) -> Result<(), DownloadError> {
    let mut url = Url::parse(&format!("{}/{}", manifest.download_url, file.name))
        .map_err(|_error| DownloadError::InvalidUrl)?;
    let mut response = None;
    for _ in 0..=MAX_REDIRECTS {
        let request = client
            .get(url.as_str())
            .map_err(|_rejected| DownloadError::DestinationRejected)?
            // The client's own timeout would bound the whole transfer; the
            // stall bound below replaces it.
            .timeout(TRANSFER_LIMIT);
        let answer = tokio::time::timeout(stall, request.send())
            .await
            .map_err(|_elapsed| DownloadError::Stalled)?
            .map_err(DownloadError::from_fetch)?;
        if !is_redirect(answer.status()) {
            response = Some(
                answer
                    .error_for_status()
                    .map_err(DownloadError::from_fetch)?,
            );
            break;
        }
        url = answer
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|location| location.to_str().ok())
            .and_then(|location| url.join(location).ok())
            .ok_or(DownloadError::BadRedirect)?;
    }
    let mut response = response.ok_or(DownloadError::BadRedirect)?;
    let mut out = tokio::fs::File::create(partial).await?;
    let mut written: u64 = 0;
    while let Some(chunk) = tokio::time::timeout(stall, response.chunk())
        .await
        .map_err(|_elapsed| DownloadError::Stalled)?
        .map_err(DownloadError::from_fetch)?
    {
        written = written.saturating_add(chunk.len() as u64);
        if written > file.size {
            return Err(DownloadError::WrongFile);
        }
        out.write_all(&chunk).await?;
    }
    out.flush().await?;
    Ok(())
}

/// Whether `status` sends the client to the `Location` header.
fn is_redirect(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

#[cfg(test)]
mod tests;
