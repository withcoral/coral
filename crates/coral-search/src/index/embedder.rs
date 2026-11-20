//! The Embedder: the third background task of [`SearchIndex`], beside the
//! backlog observer and the reconcile task. It gives each Search Entity a
//! vector of its text (ADR 0035, "The Embedder").
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
//! The model runs only for a text that has no vector in the same Workspace.
//! Each vector row keeps the SHA-256 of the text it was made from. Before
//! the model runs on a batch, a row whose own vector was made from its text
//! keeps that vector and takes the row's new stamp, and a row whose text
//! another row's vector was made from takes that vector through the same
//! write as a model vector. A new Generation of a Provider whose Tools did
//! not change, or a row written again with the same text, so runs no model.
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
use std::time::{Duration, Instant};

use coral_db::UnixNanos;
use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_telemetry::metric::{self, EmbedBatchOutcome};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, oneshot, watch};
use tracing::Instrument as _;
use tracing::field::Empty;

use super::SearchIndex;
use crate::index_text::{IndexTextSource, index_text};
use crate::indexer::{self, IndexError};

use repository::{EntityText, EntityVector, StaleEntity, StampMove, TextSha256};

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
pub(crate) type EmbedderModel = watch::Receiver<crate::ModelLoad>;

/// What the Embedder of one Coral Instance shares across its restarts and
/// with the index writes that wake it.
pub(super) struct EmbedderState {
    model: EmbedderModel,
    wake: Notify,
    /// How many times something woke the Embedder, boot counting as one.
    requested: watch::Sender<u64>,
    /// The highest wake count a finished drain started after. Only tests
    /// read it, through [`Self::drained`].
    #[cfg(any(test, feature = "test-fixtures"))]
    drained: watch::Sender<u64>,
    /// Rows that failed while others succeeded, by key, with the stamp of
    /// the row that failed.
    skipped: std::sync::Mutex<HashMap<EntityKey, UnixNanos>>,
}

impl EmbedderState {
    pub(super) fn new(model: EmbedderModel) -> Self {
        Self {
            model,
            wake: Notify::new(),
            // The drain at boot answers the first wake.
            requested: watch::Sender::new(1),
            #[cfg(any(test, feature = "test-fixtures"))]
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
    #[cfg(any(test, feature = "test-fixtures"))]
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
            .wait_for(|model| model.model().is_ok())
            .await
            .ok()
            .and_then(|model| model.model().ok());
        match ready {
            Some(model) => model,
            None => std::future::pending().await,
        }
    }

    /// How many rows are on the skip list.
    fn skipped_count(&self) -> u64 {
        u64::try_from(self.skipped().len()).unwrap_or(u64::MAX)
    }

    fn skipped(&self) -> std::sync::MutexGuard<'_, HashMap<EntityKey, UnixNanos>> {
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
            Some(stamp) if *stamp == entity.indexed_at => true,
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
            skipped.insert(EntityKey::of(&entity), entity.indexed_at);
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
            #[cfg(any(test, feature = "test-fixtures"))]
            state
                .drained
                .send_modify(|drained| *drained = (*drained).max(target));
            if *state.requested.borrow() == target {
                state.wake.notified().await;
            }
        }
    }

    /// Runs the Embedder without the reconcile task beside it. That task
    /// starts a pass as it starts, beside whatever a test does next. A test
    /// that says when each pass runs starts the Embedder with this instead.
    #[cfg(test)]
    pub(crate) fn spawn_embedder(&self) -> tokio::task::JoinHandle<()> {
        use tracing::instrument::WithSubscriber as _;

        let index = self.clone();
        let embed = async move {
            if let Some(state) = index.embedder.clone() {
                index.embed_loop(state).await;
            }
        };
        tokio::spawn(embed.with_current_subscriber())
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
            coral.search.embedder.vectors_reused = Empty,
            coral.search.embedder.vectors_embedded = Empty,
            coral.search.embedder.vectors_written = Empty,
            error.type = Empty,
        );
        let started = Instant::now();
        let written = self
            .embed_rows_in_span(thread, workspace_id, entities)
            .instrument(span.clone())
            .await;
        metric::embed_batch_ended(
            if matches!(written, Ok(Ok(_))) {
                EmbedBatchOutcome::Ok
            } else {
                EmbedBatchOutcome::Failed
            },
            started.elapsed(),
        );
        // The thread is gone with the batch, as when the model panics.
        let written = written.inspect_err(|_gone| {
            coral_telemetry::record_failure(&span, "thread_gone");
        })?;
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
        let texts = rows.iter().map(entity_index_text).collect::<Vec<_>>();
        let digests = texts
            .iter()
            .map(|text| text_sha256(text))
            .collect::<Vec<_>>();
        let revision = MANIFEST.embedder_revision();
        let reusable = self
            .read_snapshot(async |transaction| {
                Reusable::read(
                    transaction,
                    workspace_id,
                    revision.as_str(),
                    &rows,
                    &digests,
                )
                .await
            })
            .await;
        let reusable = match reusable {
            Ok(reusable) => reusable,
            Err(error) => return Ok(Err(BatchError::Index(error))),
        };
        let BatchPlan {
            moves,
            copies,
            embedded,
            texts,
        } = reusable.plan(&rows, texts, &digests);
        // Counted as planned: the write's staleness check may leave out a
        // row of either kind.
        let span = tracing::Span::current();
        span.record(
            "coral.search.embedder.vectors_reused",
            safe::number(moves.len() + copies.len()),
        );
        span.record(
            "coral.search.embedder.vectors_embedded",
            safe::number(texts.len()),
        );
        let model_vectors = thread.embed(texts).await?;
        let model_vectors = match model_vectors.into_iter().collect::<Result<Vec<_>, _>>() {
            Ok(vectors) if vectors.iter().all(|vector| finite(vector)) => vectors,
            Ok(_) | Err(_) => return Ok(Err(BatchError::Embed)),
        };
        let mut vectors = copies;
        vectors.extend(
            embedded
                .iter()
                .zip(&model_vectors)
                .map(|((row, digest), vector)| EntityVector {
                    doc_id: &row.doc_id,
                    indexed_at: row.indexed_at,
                    text_sha256: digest,
                    vector,
                }),
        );
        let written = self
            .write_batch(workspace_id, revision.as_str(), &moves, &vectors)
            .await;
        Ok(written.map_err(BatchError::Index))
    }

    /// Writes one batch in one transaction under the Workspace's index lock:
    /// moves each stamp of `moves` onto its row's own vector, and writes
    /// `vectors`. Returns the vectors made current.
    async fn write_batch(
        &self,
        workspace_id: &WorkspaceId,
        revision: &str,
        moves: &[StampMove<'_>],
        vectors: &[EntityVector<'_>],
    ) -> Result<u64, IndexError> {
        self.within_limit(async {
            let mut transaction = self.database.begin().await?;
            indexer::lock_workspace(&mut transaction, workspace_id).await?;
            let restamped =
                repository::move_stamps(&mut transaction, workspace_id, revision, moves).await?;
            let written =
                repository::write_vectors(&mut transaction, workspace_id, revision, vectors)
                    .await?;
            transaction.commit().await?;
            Ok(restamped + written)
        })
        .await
    }

    /// The Embedder's work for the backlog observer: the rows that need a
    /// vector, and the rows on the skip list. `None` while the vector side
    /// is off.
    pub(super) async fn embedder_backlog(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
    ) -> Result<Option<metric::EmbedderBacklog>, IndexError> {
        let Some(embedder) = &self.embedder else {
            return Ok(None);
        };
        Ok(Some(metric::EmbedderBacklog {
            stale: repository::count_stale(transaction, MANIFEST.embedder_revision().as_str())
                .await?,
            skipped: embedder.skipped_count(),
        }))
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

/// What one batch can take from the vectors its Workspace stores.
struct Reusable {
    /// The rows, by document id, whose own vector was made from the text
    /// they hold now.
    own_matches: HashSet<String>,
    /// For the other rows, a stored vector of another row made from the
    /// same text, by the text's digest.
    by_text: HashMap<TextSha256, Vec<f32>>,
}

impl Reusable {
    /// Reads, from the vectors of `revision` in the Workspace, the vectors
    /// `rows` can reuse. `digests` holds the digest of each row's text.
    async fn read(
        transaction: &mut coral_db::Transaction<'_>,
        workspace_id: &WorkspaceId,
        revision: &str,
        rows: &[EntityText],
        digests: &[TextSha256],
    ) -> Result<Self, IndexError> {
        let stored =
            repository::vectors_of_texts(transaction, workspace_id, revision, digests).await?;
        let own_matches = rows
            .iter()
            .zip(digests)
            .filter(|(row, digest)| {
                stored
                    .iter()
                    .any(|stored| stored.doc_id == row.doc_id && stored.text_sha256 == **digest)
            })
            .map(|(row, _digest)| row.doc_id.clone())
            .collect::<HashSet<_>>();
        let wanted = rows
            .iter()
            .zip(digests)
            .filter(|(row, _digest)| !own_matches.contains(&row.doc_id))
            .map(|(_row, digest)| digest)
            .collect::<HashSet<_>>();
        // One source for each text: each vector made from it is the same.
        let mut sources = HashMap::new();
        for stored in &stored {
            if wanted.contains(&stored.text_sha256) {
                sources
                    .entry(stored.text_sha256)
                    .or_insert(stored.doc_id.as_str());
            }
        }
        let doc_ids = sources.values().copied().collect::<Vec<_>>();
        let mut vectors = repository::stored_vectors(transaction, workspace_id, &doc_ids)
            .await?
            .into_iter()
            .map(|stored| (stored.doc_id, stored.vector))
            .collect::<HashMap<_, _>>();
        let by_text = sources
            .into_iter()
            .filter_map(|(digest, doc_id)| vectors.remove(doc_id).map(|vector| (digest, vector)))
            .collect();
        Ok(Self {
            own_matches,
            by_text,
        })
    }

    /// Sorts `rows`, whose texts are `texts` and their digests `digests`,
    /// by where each gets its vector.
    fn plan<'a>(
        &'a self,
        rows: &'a [EntityText],
        texts: Vec<String>,
        digests: &'a [TextSha256],
    ) -> BatchPlan<'a> {
        let mut plan = BatchPlan {
            moves: Vec::new(),
            copies: Vec::new(),
            embedded: Vec::new(),
            texts: Vec::new(),
        };
        for ((row, text), digest) in rows.iter().zip(texts).zip(digests) {
            if self.own_matches.contains(&row.doc_id) {
                plan.moves.push(StampMove {
                    doc_id: &row.doc_id,
                    indexed_at: row.indexed_at,
                    text_sha256: digest,
                });
            } else if let Some(vector) = self.by_text.get(digest) {
                plan.copies.push(EntityVector {
                    doc_id: &row.doc_id,
                    indexed_at: row.indexed_at,
                    text_sha256: digest,
                    vector,
                });
            } else {
                plan.embedded.push((row, digest));
                plan.texts.push(text);
            }
        }
        plan
    }
}

/// Where each row of one batch gets its vector.
struct BatchPlan<'a> {
    /// The rows whose own vector stays and takes the row's stamp.
    moves: Vec<StampMove<'a>>,
    /// The rows that take another row's stored vector.
    copies: Vec<EntityVector<'a>>,
    /// The rows the model embeds, with their digests, in the order of
    /// `texts`.
    embedded: Vec<(&'a EntityText, &'a TextSha256)>,
    /// The index text of each row of `embedded`, for the model.
    texts: Vec<String>,
}

/// The SHA-256 of an index text, as its vector row keeps it.
fn text_sha256(text: &str) -> TextSha256 {
    Sha256::digest(text.as_bytes()).into()
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

/// Whether `vector` holds only finite numbers. pgvector and sqlite-vec
/// refuse any other; that is the text's failure, not the database's.
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
