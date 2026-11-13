//! The Embedder: the third background task of [`SearchIndex`], beside the
//! backlog observer and the reconcile task. It gives each Search Entity a
//! vector of its text on Postgres (ADR 0035, "The Embedder").
//!
//! It drains a predicate, not a queue: a Search Entity needs a vector when it
//! has none, or one of another Embedder revision, or one made from another
//! source stamp than its row's. A drain reads such rows oldest write first,
//! embeds them in batches on one low-priority OS thread, and writes each batch
//! in one transaction under the Workspace's index lock. The write checks
//! staleness again, so two Embedders on the same rows write each vector once.
//! An index write wakes it, so does each reconcile pass, and it drains once
//! when the model is ready.
//!
//! A batch that fails is retried once after a pause. If the database failed,
//! the drain stops and the next wake tries again. If the model failed, the
//! Embedder embeds a fixed probe text: when the model fails that too, it is
//! down, and the drain stops. Otherwise each row of the batch is tried alone,
//! and a row whose text the model still refuses goes on an in-memory skip list
//! until its row's stamp changes or the process restarts, so it does not hold
//! back the rows behind it.

mod repository;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;
use tokio::sync::{Notify, oneshot, watch};
use tracing::Instrument as _;
use tracing::field::Empty;

use super::SearchIndex;
use crate::index_text::{IndexTextSource, index_text};
use crate::indexer::{self, IndexError};

use repository::{EntityText, EntityVector, StaleEntity};

/// The rows one write holds at most.
const BATCH_ROWS: usize = 32;

/// The stale rows one read of a drain takes, beside the rows the last read
/// returned that the drain leaves out.
const READ_ROWS: usize = BATCH_ROWS * 32;

/// The pause before a failed batch's retry.
pub(super) const RETRY_PAUSE: Duration = Duration::from_secs(5);

/// A text any working model embeds: a batch whose texts fail while this one
/// does not holds a text the model refuses.
const PROBE_TEXT: &str = "Coral";

/// The model a running Embedder embeds with, once it is ready.
pub(crate) type EmbedderModel = watch::Receiver<Option<Arc<dyn Embed>>>;

/// What the Embedder of one Coral Instance shares across its restarts and
/// with the index writes that wake it.
pub(super) struct EmbedderState {
    model: EmbedderModel,
    wake: Notify,
    /// How many times something woke the Embedder, boot counting as one.
    requested: watch::Sender<u64>,
    /// The highest wake count a finished drain started after.
    drained: watch::Sender<u64>,
    /// Rows that failed while others succeeded, by key, with the stamp of
    /// the row that failed.
    skipped: std::sync::Mutex<HashMap<EntityKey, i64>>,
}

impl EmbedderState {
    pub(super) fn new(model: EmbedderModel) -> Self {
        Self {
            model,
            wake: Notify::new(),
            // The drain at boot answers the first wake.
            requested: watch::Sender::new(1),
            drained: watch::Sender::new(0),
            skipped: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Asks for a drain that starts after this call.
    pub(super) fn wake(&self) {
        self.requested.send_modify(|requested| *requested += 1);
        self.wake.notify_one();
    }

    /// Waits until a drain that started after every wake before this call
    /// has finished.
    pub(super) async fn drained(&self) {
        let target = *self.requested.borrow();
        let mut drained = self.drained.subscribe();
        // This state holds the sender, so the channel stays open.
        drop(drained.wait_for(|drained| *drained >= target).await);
    }

    /// The model, once it is ready. Never returns when the model's source
    /// ends without one.
    async fn ready_model(&self) -> Arc<dyn Embed> {
        let mut model = self.model.clone();
        let ready = model
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|model| model.clone());
        match ready {
            Some(model) => model,
            None => std::future::pending().await,
        }
    }

    fn skipped(&self) -> std::sync::MutexGuard<'_, HashMap<EntityKey, i64>> {
        // No code panics while holding this lock, so a poisoned one still
        // holds a valid list.
        self.skipped.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether `entity` is on the skip list at its row's current stamp. An
    /// entry of another stamp leaves the list: the row changed.
    fn is_skipped(&self, entity: &StaleEntity) -> bool {
        let key = EntityKey::of(entity);
        let mut skipped = self.skipped();
        match skipped.get(&key) {
            Some(stamp) if *stamp == entity.indexed_at_unix_nanos => true,
            Some(_) => {
                skipped.remove(&key);
                false
            }
            None => false,
        }
    }

    /// Forgets each skipped row that is not in `stale`, every stale row
    /// there is: the row went, or it got its vector.
    fn keep_skipped(&self, stale: &[StaleEntity]) {
        let stale = stale.iter().map(EntityKey::of).collect::<HashSet<_>>();
        self.skipped().retain(|key, _stamp| stale.contains(key));
    }

    fn skip(&self, entities: impl IntoIterator<Item = StaleEntity>) {
        let mut skipped = self.skipped();
        for entity in entities {
            tracing::warn!(
                coral.workspace.id = safe::id(&entity.workspace_id),
                "skipping a Search Entity whose vector keeps failing until its row changes"
            );
            skipped.insert(EntityKey::of(&entity), entity.indexed_at_unix_nanos);
        }
    }
}

/// A Search Entity's key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EntityKey {
    workspace_id: WorkspaceId,
    doc_id: String,
}

impl EntityKey {
    fn of(entity: &StaleEntity) -> Self {
        Self {
            workspace_id: entity.workspace_id.clone(),
            doc_id: entity.doc_id.clone(),
        }
    }
}

impl SearchIndex {
    /// Runs the Embedder until the caller aborts it: waits for the model,
    /// then drains once and again after each wake. Returns when its OS
    /// thread is gone, so its supervisor starts it again with a new one.
    pub(super) async fn embed_loop(&self, state: Arc<EmbedderState>) {
        let model = state.ready_model().await;
        let thread = match EmbedThread::spawn(model) {
            Ok(thread) => thread,
            Err(error) => {
                tracing::error!(
                    error.type = safe::kind(io_error_kind(&error)),
                    "failed to start the search Embedder's thread"
                );
                return;
            }
        };
        loop {
            let target = *state.requested.borrow();
            if self.drain(&state, &thread).await.is_err() {
                tracing::error!(
                    error.type = safe::kind("thread_gone"),
                    "the search Embedder's thread stopped"
                );
                return;
            }
            state
                .drained
                .send_modify(|drained| *drained = (*drained).max(target));
            if *state.requested.borrow() == target {
                state.wake.notified().await;
            }
        }
    }

    /// Embeds every stale row the skip list leaves, until none is left or a
    /// batch finds the database or the model down.
    async fn drain(&self, state: &EmbedderState, thread: &EmbedThread) -> Result<(), ThreadGone> {
        let revision = MANIFEST.embedder_revision();
        let mut attempted = HashSet::new();
        // A written row leaves the stale rows, so the next read takes only
        // as many more rows as this read returned and left out.
        let mut passed_over = 0;
        loop {
            let limit = READ_ROWS + passed_over;
            let found = self
                .read_snapshot(async |transaction| {
                    repository::stale_entities(transaction, revision.as_str(), limit).await
                })
                .await;
            let found = match found {
                Ok(found) => found,
                Err(error) => {
                    tracing::error!(
                        error.type = safe::kind(error.code()),
                        "failed to read the Search Entities that need a vector"
                    );
                    return Ok(());
                }
            };
            let pending = found
                .iter()
                .filter(|entity| !attempted.contains(*entity) && !state.is_skipped(entity))
                .cloned()
                .collect::<Vec<_>>();
            passed_over = found.len() - pending.len();
            if pending.is_empty() {
                if found.len() < limit {
                    // This read returned every stale row.
                    state.keep_skipped(&found);
                    return Ok(());
                }
                // Every row read is one to leave out: read past them.
                continue;
            }
            for batch in batches(&pending) {
                attempted.extend(batch.iter().cloned());
                match self.embed_batch(thread, batch).await? {
                    BatchOutcome::Written { refused } => state.skip(refused),
                    BatchOutcome::Down => return Ok(()),
                }
            }
        }
    }

    /// Embeds and writes one batch, retrying it once after a pause. When
    /// the model fails the batch but embeds [`PROBE_TEXT`], tries each row
    /// alone and answers the rows whose text the model refuses.
    async fn embed_batch(
        &self,
        thread: &EmbedThread,
        batch: &[StaleEntity],
    ) -> Result<BatchOutcome, ThreadGone> {
        let written = Ok(BatchOutcome::Written {
            refused: Vec::new(),
        });
        if self.embed_rows(thread, batch).await?.is_ok() {
            return written;
        }
        tokio::time::sleep(self.embedder_retry_pause).await;
        match self.embed_rows(thread, batch).await? {
            Ok(()) => return written,
            Err(BatchError::Index(_)) => return Ok(BatchOutcome::Down),
            Err(BatchError::Embed) => {}
        }
        let probe = thread.embed(vec![PROBE_TEXT.to_owned()]).await?;
        if !probe
            .iter()
            .all(|vector| vector.as_deref().is_ok_and(finite))
        {
            tracing::warn!(
                error.type = safe::kind("model_down"),
                "the search model embeds no text; the Embedder tries again at its next wake"
            );
            return Ok(BatchOutcome::Down);
        }
        let mut refused = Vec::new();
        for entity in batch {
            match self
                .embed_rows(thread, std::slice::from_ref(entity))
                .await?
            {
                Ok(()) => {}
                Err(BatchError::Embed) => refused.push(entity.clone()),
                Err(BatchError::Index(_)) => return Ok(BatchOutcome::Down),
            }
        }
        Ok(BatchOutcome::Written { refused })
    }

    /// Reads the rows of `entities`, which share one Workspace, as they stand
    /// now, embeds their text and writes their vectors in one transaction
    /// under the Workspace's index lock. A row that changed or went since the
    /// drain read it is embedded as it stands, or left out.
    async fn embed_rows(
        &self,
        thread: &EmbedThread,
        entities: &[StaleEntity],
    ) -> Result<Result<(), BatchError>, ThreadGone> {
        let Some(workspace_id) = entities.first().map(|entity| &entity.workspace_id) else {
            return Ok(Ok(()));
        };
        let span = tracing::info_span!(
            parent: None,
            "coral.search.embed_batch",
            coral.workspace.id = safe::id(workspace_id),
            coral.search.embedder.rows = Empty,
            coral.search.embedder.vectors_written = Empty,
            error.type = Empty,
        );
        let written = self
            .embed_rows_in_span(thread, workspace_id, entities)
            .instrument(span.clone())
            .await?;
        match &written {
            Ok(vectors) => {
                span.record(
                    "coral.search.embedder.vectors_written",
                    safe::number(*vectors),
                );
            }
            Err(error) => {
                coral_telemetry::record_failure(&span, error.code());
                tracing::warn!(
                    parent: &span,
                    coral.workspace.id = safe::id(workspace_id),
                    error.type = safe::kind(error.code()),
                    "failed to embed a batch of Search Entities"
                );
            }
        }
        Ok(written.map(drop))
    }

    async fn embed_rows_in_span(
        &self,
        thread: &EmbedThread,
        workspace_id: &WorkspaceId,
        entities: &[StaleEntity],
    ) -> Result<Result<u64, BatchError>, ThreadGone> {
        let doc_ids = entities
            .iter()
            .map(|entity| entity.doc_id.as_str())
            .collect::<Vec<_>>();
        let rows = self
            .read_snapshot(async |transaction| {
                repository::entity_texts(transaction, workspace_id, &doc_ids).await
            })
            .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => return Ok(Err(BatchError::Index(error))),
        };
        tracing::Span::current().record("coral.search.embedder.rows", safe::number(rows.len()));
        if rows.is_empty() {
            return Ok(Ok(0));
        }
        let texts = rows.iter().map(entity_index_text).collect();
        let vectors = thread.embed(texts).await?;
        let vectors = match vectors.into_iter().collect::<Result<Vec<_>, _>>() {
            Ok(vectors) if vectors.iter().all(|vector| finite(vector)) => vectors,
            Ok(_) | Err(_) => return Ok(Err(BatchError::Embed)),
        };
        let vectors = rows
            .iter()
            .zip(&vectors)
            .map(|(row, vector)| EntityVector {
                doc_id: &row.doc_id,
                indexed_at_unix_nanos: row.indexed_at_unix_nanos,
                vector,
            })
            .collect::<Vec<_>>();
        let revision = MANIFEST.embedder_revision();
        let written = self
            .within_limit(async {
                let mut transaction = self.database.begin().await?;
                indexer::lock_workspace(&mut transaction, workspace_id).await?;
                let written = repository::write_vectors(
                    &mut transaction,
                    workspace_id,
                    revision.as_str(),
                    &vectors,
                )
                .await?;
                transaction.commit().await?;
                Ok(written)
            })
            .await;
        Ok(written.map_err(BatchError::Index))
    }

    /// Removes each vector whose Search Entity or Workspace is gone. The
    /// caller runs it only while the vector side is on. Best effort.
    pub(super) async fn remove_orphaned_vectors(&self) -> Result<(), ()> {
        let removed = self
            .within_limit(async {
                let mut transaction = self.database.begin().await?;
                let removed = repository::delete_orphaned_vectors(&mut transaction).await?;
                transaction.commit().await?;
                Ok(removed)
            })
            .await;
        if let Err(error) = removed {
            tracing::error!(
                error.type = safe::kind(error.code()),
                "failed to remove orphaned search vectors"
            );
            return Err(());
        }
        Ok(())
    }

    /// Wakes the Embedder, when the vector side is on.
    pub(super) fn wake_embedder(&self) {
        if let Some(embedder) = &self.embedder {
            embedder.wake();
        }
    }
}

/// `entities` cut into batches of one Workspace each, in order.
fn batches(entities: &[StaleEntity]) -> Vec<&[StaleEntity]> {
    let mut batches = Vec::new();
    let mut rest = entities;
    while let Some(first) = rest.first() {
        let length = rest
            .iter()
            .take(BATCH_ROWS)
            .take_while(|entity| entity.workspace_id == first.workspace_id)
            .count();
        let (batch, after) = rest.split_at(length);
        batches.push(batch);
        rest = after;
    }
    batches
}

fn entity_index_text(row: &EntityText) -> String {
    index_text(&IndexTextSource {
        kind: row.kind,
        qualified_name: &row.qualified_name,
        title: &row.title,
        description: &row.description,
        provider_name: &row.provider_name,
        field_names: &row.field_names,
    })
}

enum BatchOutcome {
    /// The batch was written but for `refused`, whose text the model refuses
    /// while it embeds other text.
    Written { refused: Vec<StaleEntity> },
    /// The database failed, or the model embeds no text.
    Down,
}

/// Whether `vector` holds only finite numbers. pgvector refuses any other;
/// that is the text's failure, not the database's.
fn finite(vector: &[f32]) -> bool {
    vector.iter().all(|value| value.is_finite())
}

/// Why one write of the Embedder failed. The model's error is not logged:
/// its text may quote the catalog.
enum BatchError {
    Embed,
    Index(IndexError),
}

impl BatchError {
    const fn code(&self) -> &'static str {
        match self {
            Self::Embed => "embed_failed",
            Self::Index(error) => error.code(),
        }
    }
}

/// The Embedder's OS thread is gone, and its jobs with it.
#[derive(Debug)]
struct ThreadGone;

/// The OS thread that runs the model for the Embedder, at low priority. It
/// stops when the Embedder drops this handle.
struct EmbedThread {
    jobs: std::sync::mpsc::Sender<EmbedJob>,
}

struct EmbedJob {
    texts: Vec<String>,
    vectors: oneshot::Sender<Vec<Result<Vec<f32>, EmbedError>>>,
}

impl EmbedThread {
    fn spawn(model: Arc<dyn Embed>) -> std::io::Result<Self> {
        let (jobs, receiver) = std::sync::mpsc::channel::<EmbedJob>();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::Builder::new()
            .name("coral-search-embedder".to_owned())
            .spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    lower_priority();
                    for job in receiver {
                        let vectors = job
                            .texts
                            .iter()
                            .map(|text| model.embed_document(text))
                            .collect();
                        // The drain that sent the job may be gone.
                        drop(job.vectors.send(vectors));
                    }
                });
            })?;
        Ok(Self { jobs })
    }

    async fn embed(
        &self,
        texts: Vec<String>,
    ) -> Result<Vec<Result<Vec<f32>, EmbedError>>, ThreadGone> {
        let (vectors, answer) = oneshot::channel();
        self.jobs
            .send(EmbedJob { texts, vectors })
            .map_err(|_closed| ThreadGone)?;
        answer.await.map_err(|_closed| ThreadGone)
    }
}

/// Lowers the calling thread's priority to the lowest its scheduling policy
/// allows, so embedding gives way to every other thread of the process.
fn lower_priority() {
    if thread_priority::set_current_thread_priority(thread_priority::ThreadPriority::Min).is_err() {
        tracing::warn!(
            error.type = safe::kind("thread_priority"),
            "failed to lower the search Embedder's thread priority"
        );
    }
}

fn io_error_kind(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::OutOfMemory => "out_of_memory",
        _ => "io",
    }
}
