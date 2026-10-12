//! Persistence encoding of accepted Account intent, not live lifecycle decisions.
use crate::Error;
use crate::account::ProviderAccountId;
use crate::account::intent::{AuthorizationIntent, Caller, ConnectionIntent};
use coral_workspace::{PrincipalId, UserId};

#[derive(sqlx::FromRow)]
pub(super) struct IntentColumns {
    pub(super) expected_account_id: String,
    pub(super) expected_authorization_version: String,
    pub(super) provider_account_setting_version: i64,
    pub(super) connection_intent: String,
    pub(super) agent_id: Option<String>,
    pub(super) user_id: Option<String>,
}
impl IntentColumns {
    pub(super) fn encode(intent: &AuthorizationIntent) -> Self {
        Self {
            expected_account_id: intent.account.to_string(),
            expected_authorization_version: intent.authorization_version.clone(),
            provider_account_setting_version: intent.provider_account_setting_version,
            connection_intent: match intent.kind {
                ConnectionIntent::Initial => "initial",
                ConnectionIntent::Replace => "replace",
            }
            .into(),
            agent_id: match &intent.caller {
                Some(Caller::Agent(id)) => Some(id.to_string()),
                _ => None,
            },
            user_id: match &intent.caller {
                Some(Caller::User(id)) => Some(id.to_string()),
                _ => None,
            },
        }
    }
    pub(super) fn decode(self, actor: &UserId) -> Result<AuthorizationIntent, Error> {
        let caller = match (self.user_id, self.agent_id) {
            (Some(user), None) if user == actor.as_str() => {
                Some(Caller::User(UserId::parse(&user)?))
            }
            (None, Some(agent)) => Some(Caller::Agent(PrincipalId::parse(&agent)?)),
            (None, None) => None,
            _ => return Err(Error::AccountInput),
        };
        if self.provider_account_setting_version <= 0
            || self.expected_authorization_version.is_empty()
        {
            return Err(Error::AccountInput);
        }
        Ok(AuthorizationIntent {
            account: ProviderAccountId::parse(&self.expected_account_id)?,
            authorization_version: self.expected_authorization_version,
            provider_account_setting_version: self.provider_account_setting_version,
            kind: match self.connection_intent.as_str() {
                "initial" => ConnectionIntent::Initial,
                "replace" => ConnectionIntent::Replace,
                _ => return Err(Error::AccountInput),
            },
            caller,
        })
    }
}
