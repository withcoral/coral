pub(crate) mod gui_onboarding;
pub(crate) mod identity_specs;
pub(crate) mod materializations;
pub(crate) mod source_manifests;
pub(crate) mod sources;
pub(crate) mod state_migrations;
pub(crate) mod task_queries;
pub(crate) mod tasks;
pub(crate) mod trace_search_responses;
pub(crate) mod users;
pub(crate) mod workspace_members;
pub(crate) mod workspaces;

#[cfg(test)]
mod identity_specs_contract_tests;
#[cfg(test)]
mod identity_specs_negative_contract_tests;
