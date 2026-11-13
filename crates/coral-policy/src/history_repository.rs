//! Exact policy selection for payload-free pending-approval projections.
use crate::{ToolAccessState, default_access_state};
use coral_catalog::{AuthoredToolKind, HttpMethod, ToolHints, ToolPolicyFacts};
use coral_workspace::PrincipalKind;
use sea_query::{
    Alias, ColumnRef, Expr, ExprTrait as _, Func, Query, SimpleExpr, SubQueryStatement,
};

/// The columns of one retained Invocation, and of the Run that made it, that
/// select its policy.
pub struct InvocationColumns {
    /// The Workspace of the Invocation.
    pub workspace: ColumnRef,
    /// The Provider of its Tool.
    pub provider: ColumnRef,
    /// The Generation it retained.
    pub generation: ColumnRef,
    /// The Tool Key it retained.
    pub tool: ColumnRef,
    /// Its Call Path, which keeps the kind of a Private or Shared Tool.
    pub call_path: ColumnRef,
    /// The default of the revision its Run linked, for a Private or Shared
    /// Tool; NULL for any other Tool.
    pub declared_default: ColumnRef,
    /// The Principal that invoked its Run.
    pub principal: ColumnRef,
    /// The kind of that Principal.
    pub principal_kind: ColumnRef,
}

/// Whether the Invocation's policy is `approval_required` now, as the Policy
/// Enforcement Point decides it: the exact rule when it applies, else the
/// default the Invocation retained.
///
/// A Provider or Built-in Tool takes the selection of
/// [`crate::invocation_decision_in_transaction`]: retained Generation
/// evidence fixes its default even after catalog publication changes. A
/// Private or Shared Tool takes that of
/// [`crate::authored_tool_decision_in_transaction`]: its default is the one
/// its Invocation retained, and a rule on a Private Tool applies only while
/// its invoking Principal, whose Tool it is, is an Agent or holds the Owner
/// role now ([`crate::PrivateToolOwner::takes_rule_on`]).
#[must_use]
pub fn approval_required(invocation: InvocationColumns) -> SimpleExpr {
    let InvocationColumns {
        workspace,
        provider,
        generation,
        tool,
        call_path,
        declared_default,
        principal,
        principal_kind,
    } = invocation;
    let private = coral_catalog::history_repository::names_authored_tool(
        AuthoredToolKind::Private,
        call_path,
    );
    let caller_takes_rules = Expr::col(principal_kind.clone())
        .eq(PrincipalKind::Agent.as_storage_str())
        .or(coral_workspace::history_repository::invoker_is_owner(
            workspace.clone(),
            principal,
            principal_kind,
        ));
    // No rule is read where none applies: the CASE is NULL there.
    let applying_rule: SimpleExpr = Expr::case(
        private.not().or(caller_takes_rules),
        exact_rule(workspace, provider, tool.clone()),
    )
    .into();
    // Only a Private or Shared Tool's Invocation retains a declared default,
    // so any other falls through to its Generation's.
    Expr::expr(Func::coalesce([
        applying_rule,
        Expr::col(declared_default),
        generation_default(generation, tool),
    ]))
    .eq(ToolAccessState::ApprovalRequired.as_storage_str())
}

/// The exact rule on the Tool, NULL when it has none.
fn exact_rule(workspace: ColumnRef, provider: ColumnRef, tool: ColumnRef) -> SimpleExpr {
    let col = |name: &str| Expr::col((Alias::new("approval_policy"), Alias::new(name)));
    Expr::SubQuery(
        None,
        Box::new(SubQueryStatement::SelectStatement(
            Query::select()
                .expr(col("access_state"))
                .from_as(
                    Alias::new("tool_access_rules"),
                    Alias::new("approval_policy"),
                )
                .and_where(col("workspace_id").eq(Expr::col(workspace)))
                .and_where(col("provider_id").eq(Expr::col(provider)))
                .and_where(col("tool_key").eq(Expr::col(tool)))
                .to_owned(),
        )),
    )
}

/// The default that the retained Generation's facts give the Tool.
fn generation_default(generation: ColumnRef, tool: ColumnRef) -> SimpleExpr {
    let read_only = ToolHints::new(Some(true), None, None, None);
    let undeclared = ToolHints::default();
    let default = |facts| Expr::val(default_access_state(facts).as_storage_str());
    Expr::case(
        coral_catalog::history_repository::declares_enabled_by_default(
            generation.clone(),
            tool.clone(),
        ),
        default(
            ToolPolicyFacts::new(undeclared, None)
                .declaring_default(Some(ToolAccessState::Enabled)),
        ),
    )
    .case(
        coral_catalog::history_repository::declares_read_only(generation.clone(), tool.clone()),
        default(ToolPolicyFacts::new(read_only, None)),
    )
    // GET stands for every safe method; the default does not tell them apart.
    .case(
        coral_catalog::history_repository::has_safe_http_method(generation, tool),
        default(ToolPolicyFacts::new(undeclared, Some(HttpMethod::Get))),
    )
    .finally(default(ToolPolicyFacts::new(undeclared, None)))
    .into()
}
