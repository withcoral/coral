//! gRPC `EpisodeService` — the `OpenEpisode` write path for trajectory-memory
//! episodes.
//!
//! Registers `{ episode_id -> intent, parent }` into the per-workspace
//! [`EpisodeStore`](super::store::EpisodeStore). Experimental: the capture that
//! drives this — opening an episode per session and tagging each query with the
//! `coral-episode-id` metadata key — is wired in `coral-mcp` in a later PR.
//!
//! Like the other local gRPC services (e.g. `FeedbackService`), the route is
//! **registered unconditionally**: the transport is feature-agnostic by design,
//! and effective features are resolved in `coral-cli`, which gates the
//! *consumers* rather than the routes. The `episodes` feature gates the only
//! caller — the `coral-mcp` capture path — so on a default/disabled install this
//! endpoint is reachable but inert: nothing opens an episode, so no `intent`
//! (possibly PII) is ever written. The always-registered write endpoint is the
//! supported contract; see the `open_episode_*` server smoke tests.

use coral_api::v1::episode_service_server::EpisodeService as EpisodeServiceApi;
use coral_api::v1::{OpenEpisodeRequest, OpenEpisodeResponse};
use tonic::{Request, Response, Status};
use tracing::warn;

use super::EpisodeId;
use super::store::{Episode, EpisodeStore, EpisodeStoreError, now_unix_nanos};
use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct EpisodeService {
    store: EpisodeStore,
}

impl EpisodeService {
    pub(crate) fn new(store: EpisodeStore) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl EpisodeServiceApi for EpisodeService {
    async fn open_episode(
        &self,
        request: Request<OpenEpisodeRequest>,
    ) -> Result<Response<OpenEpisodeResponse>, Status> {
        let span = grpc_span(&request);
        let store = self.store.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            let id = EpisodeId::parse(&request.episode_id).map_err(app_status)?;
            let intent = non_blank("intent", &request.intent)?;
            // Per AIP-149, an empty `parent_episode_id` means "no parent" (a root
            // episode); any other value must satisfy the `EpisodeId` contract.
            let parent_episode_id = match request.parent_episode_id.as_str() {
                "" => None,
                parent => Some(EpisodeId::parse(parent).map_err(app_status)?),
            };
            let episode = Episode {
                id,
                workspace,
                intent,
                parent_episode_id,
                created_at_unix_nanos: now_unix_nanos(),
            };
            store
                .open_episode(&episode)
                .map_err(|error| open_episode_status(&error))?;
            Ok(Response::new(OpenEpisodeResponse {}))
        })
        .await
    }
}

/// Trims `value` and rejects a blank result with `INVALID_ARGUMENT`.
fn non_blank(field: &str, value: &str) -> Result<String, Status> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!(
            "missing string argument '{field}'"
        )));
    }
    Ok(trimmed.to_string())
}

/// Maps an [`EpisodeStoreError`] to a gRPC [`Status`]. A reopen with a different
/// intent/parent is a client-visible precondition failure; IO/serialization
/// faults are internal and logged rather than surfaced.
fn open_episode_status(error: &EpisodeStoreError) -> Status {
    match error {
        EpisodeStoreError::Conflict { .. } => Status::failed_precondition(error.to_string()),
        EpisodeStoreError::InvalidIntent { .. } => Status::invalid_argument(error.to_string()),
        EpisodeStoreError::Io(_) | EpisodeStoreError::Serde(_) => {
            warn!(%error, "failed to persist episode");
            Status::internal("failed to persist episode")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use coral_api::v1::{OpenEpisodeRequest, Workspace};
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::super::store::EpisodeStore;
    use super::{EpisodeService, EpisodeServiceApi};
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    fn service() -> (TempDir, AppStateLayout, EpisodeService) {
        let dir = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(dir.path().join("coral-config")))
            .expect("layout should resolve");
        let service = EpisodeService::new(EpisodeStore::new(layout.clone()));
        (dir, layout, service)
    }

    fn request(
        workspace: Option<&str>,
        id: &str,
        intent: &str,
        parent: Option<&str>,
    ) -> Request<OpenEpisodeRequest> {
        Request::new(OpenEpisodeRequest {
            workspace: workspace.map(|name| Workspace {
                name: name.to_string(),
            }),
            episode_id: id.to_string(),
            intent: intent.to_string(),
            parent_episode_id: parent.unwrap_or_default().to_string(),
        })
    }

    #[tokio::test]
    async fn open_episode_persists_record() {
        let (_dir, layout, service) = service();
        service
            .open_episode(request(
                Some("acme"),
                "ep_1",
                "find the HR onboarding form",
                None,
            ))
            .await
            .expect("open episode");

        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let raw = fs::read_to_string(layout.episodes_file(&workspace)).expect("episode file");
        let records: Vec<_> = raw.lines().filter(|line| !line.trim().is_empty()).collect();
        assert_eq!(records.len(), 1);
        let record = records.first().expect("one record");
        assert!(record.contains("ep_1"));
        assert!(record.contains("find the HR onboarding form"));
    }

    #[tokio::test]
    async fn open_child_episode_records_parent() {
        let (_dir, layout, service) = service();
        service
            .open_episode(request(Some("acme"), "parent_ep", "root task", None))
            .await
            .expect("open parent");
        service
            .open_episode(request(
                Some("acme"),
                "child_ep",
                "sub task",
                Some("parent_ep"),
            ))
            .await
            .expect("open child");

        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let raw = fs::read_to_string(layout.episodes_file(&workspace)).expect("episode file");
        assert!(raw.contains(r#""parent_episode_id":"parent_ep""#));
    }

    #[tokio::test]
    async fn reopen_identical_is_ok() {
        let (_dir, _layout, service) = service();
        service
            .open_episode(request(Some("acme"), "ep_1", "same intent", None))
            .await
            .expect("first open");
        service
            .open_episode(request(Some("acme"), "ep_1", "same intent", None))
            .await
            .expect("idempotent reopen");
    }

    #[tokio::test]
    async fn reopen_with_different_intent_conflicts() {
        let (_dir, _layout, service) = service();
        service
            .open_episode(request(Some("acme"), "ep_1", "intent A", None))
            .await
            .expect("first open");
        let status = service
            .open_episode(request(Some("acme"), "ep_1", "intent B", None))
            .await
            .expect_err("changed intent must conflict");
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn missing_workspace_is_invalid_argument() {
        let (_dir, _layout, service) = service();
        let status = service
            .open_episode(request(None, "ep_1", "intent", None))
            .await
            .expect_err("missing workspace must be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn blank_episode_id_is_invalid_argument() {
        let (_dir, _layout, service) = service();
        let status = service
            .open_episode(request(Some("acme"), "   ", "intent", None))
            .await
            .expect_err("blank episode_id must be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn non_ascii_episode_id_is_invalid_argument() {
        let (_dir, _layout, service) = service();
        // A non-ASCII id cannot round-trip as the `coral-episode-id` metadata
        // value, so the boundary rejects it before it reaches the store.
        let status = service
            .open_episode(request(Some("acme"), "épisode", "intent", None))
            .await
            .expect_err("non-ASCII episode_id must be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
