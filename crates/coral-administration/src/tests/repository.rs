//! Private, backend-specific failure injection for atomic lifecycle evidence.

use coral_test_support::TestDatabase;
use sqlx::SqlSafeStr as _;

#[derive(Clone, Copy)]
pub(super) enum EventFault {
    AuthorizationCompleted,
    AccountDeleted,
    FunctionDeleted,
    MembershipRemoved,
    AgentDeleted,
    SetupCanceled,
    OrganizationMembershipRemoved,
    WorkspaceCreated,
    BuiltinProviderCreated,
}
impl EventFault {
    const ALL: [Self; 9] = [
        Self::AuthorizationCompleted,
        Self::AccountDeleted,
        Self::FunctionDeleted,
        Self::MembershipRemoved,
        Self::AgentDeleted,
        Self::SetupCanceled,
        Self::OrganizationMembershipRemoved,
        Self::WorkspaceCreated,
        Self::BuiltinProviderCreated,
    ];

    const fn target(self) -> (&'static str, &'static str, char) {
        match self {
            Self::AuthorizationCompleted => (
                "provider_lifecycle_events",
                "provider_account_authorization.completed",
                'b',
            ),
            Self::AccountDeleted => ("provider_lifecycle_events", "provider_account.deleted", 'c'),
            Self::FunctionDeleted => (
                "saved_function_lifecycle_events",
                "saved_function.deleted",
                'd',
            ),
            Self::MembershipRemoved => (
                "workspace_lifecycle_events",
                "workspace_membership.removed",
                'f',
            ),
            Self::AgentDeleted => ("workspace_lifecycle_events", "agent.deleted", 'a'),
            Self::SetupCanceled => ("provider_lifecycle_events", "provider_setup.canceled", '8'),
            Self::OrganizationMembershipRemoved => (
                "organization_lifecycle_events",
                "organization_membership.removed",
                '9',
            ),
            Self::WorkspaceCreated => ("workspace_lifecycle_events", "workspace.created", '7'),
            Self::BuiltinProviderCreated => (
                "provider_lifecycle_events",
                "provider.builtin_installed",
                '6',
            ),
        }
    }

    pub(super) fn assert_error(self, error: &crate::Error) {
        let matches = match self {
            Self::AuthorizationCompleted
            | Self::AccountDeleted
            | Self::SetupCanceled
            | Self::BuiltinProviderCreated => {
                matches!(
                    error,
                    crate::Error::Provider(coral_provider::Error::Database(_))
                )
            }
            Self::FunctionDeleted => {
                matches!(
                    error,
                    crate::Error::Functions(coral_functions::Error::Database(_))
                )
            }
            Self::MembershipRemoved
            | Self::AgentDeleted
            | Self::OrganizationMembershipRemoved
            | Self::WorkspaceCreated => {
                matches!(
                    error,
                    crate::Error::Workspace(coral_workspace::Error::Database(_))
                )
            }
        };
        assert!(matches, "unexpected injected-failure result: {error:?}");
    }

    pub(super) fn context(
        self,
        actor: &coral_workspace::AuthenticatedUser,
    ) -> coral_workspace::ChangeContext {
        coral_workspace::ChangeContext::new(
            actor.clone(),
            200,
            coral_workspace::TraceId::parse(&self.target().2.to_string().repeat(32)).unwrap(),
        )
    }
}

pub(super) async fn fixture() -> TestDatabase {
    let mut faults = Vec::new();
    let event_fault = match coral_test_support::selected_database().unwrap() {
        coral_db::DatabaseSystem::Sqlite => {
            r"CREATE TRIGGER offboarding_event_failure
            BEFORE INSERT ON saved_function_lifecycle_events
            WHEN NEW.action = 'saved_function.deleted'
                AND NEW.trace_id = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                AND EXISTS (
                    SELECT 1 FROM saved_function_lifecycle_events
                    WHERE action = 'saved_function.deleted' AND trace_id = NEW.trace_id
                )
            BEGIN
                SELECT RAISE(ABORT, 'injected second deletion event failure');
            END;"
        }
        coral_db::DatabaseSystem::Postgres => {
            r"CREATE FUNCTION offboarding_event_failure() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.action = 'saved_function.deleted'
                    AND NEW.trace_id = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    AND EXISTS (
                        SELECT 1 FROM saved_function_lifecycle_events
                        WHERE action = 'saved_function.deleted' AND trace_id = NEW.trace_id
                    )
                THEN
                    RAISE EXCEPTION 'injected second deletion event failure';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER offboarding_event_failure
                BEFORE INSERT ON saved_function_lifecycle_events
                FOR EACH ROW EXECUTE FUNCTION offboarding_event_failure();"
        }
    };
    faults.push(sqlx::migrate::Migration::new(
        9000,
        "offboarding event fault".into(),
        sqlx::migrate::MigrationType::Simple,
        sqlx::SqlStr::from_static(event_fault),
        false,
    ));
    // Fail the actual owning-domain insert, not a mocked facet result.
    for (index, fault) in EventFault::ALL.into_iter().enumerate() {
        let (table, action, trace) = fault.target();
        let name = format!("offboarding_facet_event_{index}");
        let trace = trace.to_string().repeat(32);
        let sql = match coral_test_support::selected_database().unwrap() {
            coral_db::DatabaseSystem::Sqlite => format!(
                "CREATE TRIGGER {name} BEFORE INSERT ON {table} WHEN NEW.action = '{action}' AND NEW.trace_id = '{trace}' BEGIN SELECT RAISE(ABORT, 'injected lifecycle failure'); END;"
            ),
            coral_db::DatabaseSystem::Postgres => format!(
                "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action = '{action}' AND NEW.trace_id = '{trace}' THEN RAISE EXCEPTION 'injected lifecycle failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {name} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {name}();"
            ),
        };
        faults.push(sqlx::migrate::Migration::new(
            9001 + i64::try_from(index).unwrap(),
            "offboarding facet event fault".into(),
            sqlx::migrate::MigrationType::Simple,
            sqlx::AssertSqlSafe(sql).into_sql_str(),
            false,
        ));
    }
    TestDatabase::new(&coral_test_support::migration_plan_with(faults))
        .await
        .unwrap()
}
