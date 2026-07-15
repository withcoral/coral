use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::identity::Principal;
use crate::state::db::{CoralDb, DbRepos};

#[derive(Clone)]
pub(crate) struct GuiOnboardingManager {
    db: Arc<CoralDb>,
}

impl GuiOnboardingManager {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn is_completed(&self, principal: &Principal) -> Result<bool, AppError> {
        let mut session = self.db.as_ref();
        session
            .gui_onboarding()
            .is_completed(principal.id().as_str())
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn complete(&self, principal: &Principal) -> Result<(), AppError> {
        let mut session = self.db.as_ref();
        session
            .gui_onboarding()
            .complete(principal.id().as_str())
            .await?;
        Ok(())
    }
}
