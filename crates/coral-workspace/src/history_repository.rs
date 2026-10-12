//! Narrow repository facet for authorized cross-Workspace history queries.
//! Only owning repositories compose SQL with this facet. Domains receive typed history.
use crate::HistoryAuthority;
use sea_query::{Alias, Expr, ExprTrait as _, Query, SelectStatement};

/// Safe label for the Workspace of independently authorized history.
#[must_use]
pub fn workspace_label(workspace: sea_query::ColumnRef) -> sea_query::SimpleExpr {
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            Query::select()
                .column((Alias::new("history_workspace"), Alias::new("display_name")))
                .from_as(
                    Alias::new("workspace_records"),
                    Alias::new("history_workspace"),
                )
                .and_where(
                    Expr::col((Alias::new("history_workspace"), Alias::new("id")))
                        .eq(Expr::col(workspace)),
                )
                .to_owned(),
        )),
    )
}

/// Safe retained Agent label for independently authorized Agent history. Deleted Agents
/// retain attribution; this never reads a User profile or grants directory access.
#[must_use]
pub fn agent_label(
    workspace: sea_query::ColumnRef,
    principal: sea_query::ColumnRef,
) -> sea_query::SimpleExpr {
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            Query::select()
                .column((Alias::new("history_agent"), Alias::new("display_name")))
                .from_as(Alias::new("workspace_agents"), Alias::new("history_agent"))
                .and_where(
                    Expr::col((Alias::new("history_agent"), Alias::new("workspace_id")))
                        .eq(Expr::col(workspace)),
                )
                .and_where(
                    Expr::col((Alias::new("history_agent"), Alias::new("id")))
                        .eq(Expr::col(principal)),
                )
                .to_owned(),
        )),
    )
}

/// Upcoming Registration expiry for independently authorized Agent work. This
/// is scheduling evidence, not authority; Users have no Registration deadline.
#[must_use]
pub fn invoker_deadline(
    workspace: sea_query::ColumnRef,
    principal: sea_query::ColumnRef,
    kind: sea_query::ColumnRef,
) -> sea_query::SimpleExpr {
    let col = |name: &str| Expr::col((Alias::new("deadline_registration"), Alias::new(name)));
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            Query::select()
                .expr(col("expires_at_unix_nanos"))
                .from_as(
                    Alias::new("agent_registrations"),
                    Alias::new("deadline_registration"),
                )
                .and_where(Expr::col(kind).eq("agent"))
                .and_where(col("workspace_id").eq(Expr::col(workspace)))
                .and_where(col("agent_id").eq(Expr::col(principal)))
                .to_owned(),
        )),
    )
}

/// Current execution authority facts for a retained invoking Principal. This
/// does not authorize the observer; `restrict` must independently scope history.
#[must_use]
pub fn invoker_active(
    workspace: sea_query::ColumnRef,
    principal: sea_query::ColumnRef,
    kind: sea_query::ColumnRef,
    now: i64,
) -> sea_query::SimpleExpr {
    use coral_db::col;
    let active_workspace = Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("workspace_records"),
                Alias::new("active_workspace"),
            )
            .and_where(col("active_workspace", "id").eq(Expr::col(workspace.clone())))
            .and_where(col("active_workspace", "state").eq("active"))
            .to_owned(),
    );
    let membership = Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("workspace_memberships"),
                Alias::new("invoker_membership"),
            )
            .and_where(col("invoker_membership", "workspace_id").eq(Expr::col(workspace.clone())))
            .and_where(col("invoker_membership", "user_id").eq(Expr::col(principal.clone())))
            .to_owned(),
    );
    let user = Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("organization_authority_records"),
                Alias::new("invoker_user"),
            )
            .and_where(col("invoker_user", "user_id").eq(Expr::col(principal.clone())))
            .and_where(col("invoker_user", "enabled").eq(1_i64))
            .and_where(col("invoker_user", "removed").eq(0_i64))
            .and_where(membership)
            .to_owned(),
    );
    let registration = Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("agent_registrations"),
                Alias::new("invoker_registration"),
            )
            .and_where(col("invoker_registration", "workspace_id").eq(Expr::col(workspace.clone())))
            .and_where(col("invoker_registration", "agent_id").eq(Expr::col(principal.clone())))
            .and_where(col("invoker_registration", "enabled").eq(1_i64))
            .and_where(
                col("invoker_registration", "expires_at_unix_nanos")
                    .is_null()
                    .or(col("invoker_registration", "expires_at_unix_nanos").gt(now)),
            )
            .to_owned(),
    );
    let agent = Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(Alias::new("workspace_agents"), Alias::new("invoker_agent"))
            .and_where(col("invoker_agent", "workspace_id").eq(Expr::col(workspace)))
            .and_where(col("invoker_agent", "id").eq(Expr::col(principal)))
            .and_where(col("invoker_agent", "deleted").eq(0_i64))
            .and_where(col("invoker_agent", "state").eq("enabled"))
            .and_where(registration)
            .to_owned(),
    );
    active_workspace.and(
        Expr::col(kind.clone())
            .eq("user")
            .and(user)
            .or(Expr::col(kind).eq("agent").and(agent)),
    )
}

/// Restricts an owning repository's selection to the exact Principal and currently
/// authorized Workspaces. Call in the same snapshot that resolved `authority`.
/// Column names are supplied by the owning repository; Workspace storage stays here.
pub fn restrict(
    query: &mut SelectStatement,
    authority: &HistoryAuthority,
    workspace: sea_query::ColumnRef,
    principal: sea_query::ColumnRef,
    kind: sea_query::ColumnRef,
) {
    let own = Expr::col(principal)
        .eq(authority.principal.id().as_str())
        .and(Expr::col(kind.clone()).eq(authority.principal.kind().as_storage_str()));
    query.and_where(if authority.include_owned_agents {
        let owned = Query::select()
            .column(Alias::new("workspace_id"))
            .from(Alias::new("workspace_memberships"))
            .and_where(Expr::col(Alias::new("user_id")).eq(authority.user.as_str()))
            .and_where(Expr::col(Alias::new("role")).eq("owner"))
            .to_owned();
        own.or(Expr::col(kind)
            .eq("agent")
            .and(Expr::col(workspace.clone()).in_subquery(owned)))
    } else {
        own
    });
    restrict_workspaces(query, authority, workspace);
}

/// As [`restrict`], for rows only a User can own, such as private Account connections.
pub fn restrict_user_rows(
    query: &mut SelectStatement,
    authority: &HistoryAuthority,
    workspace: sea_query::ColumnRef,
    user: sea_query::ColumnRef,
) {
    query.and_where(
        if authority.principal.kind() == crate::PrincipalKind::User {
            Expr::col(user).eq(authority.principal.id().as_str())
        } else {
            Expr::val(1_i64).eq(0_i64)
        },
    );
    restrict_workspaces(query, authority, workspace);
}

fn restrict_workspaces(
    query: &mut SelectStatement,
    authority: &HistoryAuthority,
    workspace: sea_query::ColumnRef,
) {
    if let Some(id) = &authority.workspace {
        query.and_where(Expr::col(workspace).eq(id.to_string()));
    } else {
        let accessible = Query::select()
            .column(Alias::new("workspace_id"))
            .from(Alias::new("workspace_memberships"))
            .and_where(Expr::col(Alias::new("user_id")).eq(authority.user.as_str()))
            .to_owned();
        query.and_where(Expr::col(workspace).in_subquery(accessible));
    }
}
