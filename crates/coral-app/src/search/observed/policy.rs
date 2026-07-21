//! Observed-values retrieval policy.

use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedValuesLiveScope {
    pub(crate) owner_source_name: String,
    pub(crate) source_name: String,
    pub(crate) source_scope_id: String,
    pub(crate) surface_kind: ObservedValuesSurfaceKind,
    pub(crate) surface_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedValuesLiveScopeLoadFailure {
    pub(crate) owner_source_name: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedValuesRetrievalPolicy {
    live_scopes: Vec<ObservedValuesLiveScope>,
    failed_sources: Vec<ObservedValuesLiveScopeLoadFailure>,
    stale_after_last_observed_days: u32,
}

impl ObservedValuesRetrievalPolicy {
    #[cfg(test)]
    pub(crate) fn new(
        live_scopes: Vec<ObservedValuesLiveScope>,
        stale_after_last_observed_days: u32,
    ) -> Self {
        Self::with_load_failures(live_scopes, Vec::new(), stale_after_last_observed_days)
    }

    pub(crate) fn with_load_failures(
        live_scopes: Vec<ObservedValuesLiveScope>,
        failed_sources: Vec<ObservedValuesLiveScopeLoadFailure>,
        stale_after_last_observed_days: u32,
    ) -> Self {
        Self {
            live_scopes,
            failed_sources,
            stale_after_last_observed_days,
        }
    }

    pub(crate) fn failed_source_count(&self) -> u32 {
        u32::try_from(self.failed_sources.len()).unwrap_or(u32::MAX)
    }

    pub(crate) fn has_load_failures(&self) -> bool {
        !self.failed_sources.is_empty()
    }

    pub(crate) fn failed_owner_source_names(&self) -> Vec<&str> {
        self.failed_sources
            .iter()
            .map(|failure| failure.owner_source_name.as_str())
            .collect()
    }

    pub(crate) fn stale_after_last_observed_days(&self) -> u32 {
        self.stale_after_last_observed_days
    }

    pub(crate) fn live_scopes(&self) -> &[ObservedValuesLiveScope] {
        &self.live_scopes
    }

    pub(crate) fn failed_sources(&self) -> &[ObservedValuesLiveScopeLoadFailure] {
        &self.failed_sources
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
    };
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

    #[test]
    fn retrieval_policy_reports_failed_sources() {
        let policy = ObservedValuesRetrievalPolicy::with_load_failures(
            vec![live_scope("github", "scope")],
            vec![
                load_failure("jira", "manifest parse failed"),
                load_failure("slack", "missing materialization"),
            ],
            30,
        );

        assert!(policy.has_load_failures());
        assert_eq!(policy.failed_source_count(), 2);
        assert_eq!(policy.failed_owner_source_names(), vec!["jira", "slack"]);
        assert_eq!(policy.failed_sources().len(), 2);
    }

    #[test]
    fn retrieval_policy_exposes_live_scopes_and_retention_days() {
        let policy = ObservedValuesRetrievalPolicy::new(vec![live_scope("github", "scope")], 14);

        assert_eq!(policy.live_scopes().len(), 1);
        assert_eq!(
            policy
                .live_scopes()
                .first()
                .expect("live scope")
                .source_scope_id,
            "scope"
        );
        assert_eq!(policy.stale_after_last_observed_days(), 14);
        assert!(!policy.has_load_failures());
    }

    fn live_scope(source_name: &str, source_scope_id: &str) -> ObservedValuesLiveScope {
        ObservedValuesLiveScope {
            owner_source_name: source_name.to_string(),
            source_name: source_name.to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
        }
    }

    fn load_failure(source_name: &str, message: &str) -> ObservedValuesLiveScopeLoadFailure {
        ObservedValuesLiveScopeLoadFailure {
            owner_source_name: source_name.to_string(),
            message: message.to_string(),
        }
    }
}
