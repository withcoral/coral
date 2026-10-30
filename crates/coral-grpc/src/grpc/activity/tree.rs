use super::{
    GrpcServices, PrivateAudience, Request, Response, Status, error, execution, intent_query,
    limit, resource, scope, task, task_identity, wire,
};
fn root_identity(name: &str) -> Result<coral_task::ActivityIdentity, Status> {
    if name.contains("/tasks/") {
        let id = task_identity(name)?;
        Ok(coral_task::ActivityIdentity::Task(id.workspace, id.task))
    } else {
        let (workspace, id) = resource(name, "runs")?;
        Ok(coral_task::ActivityIdentity::Run(
            workspace,
            coral_run::RunId::parse(id).map_err(execution::run_error)?,
        ))
    }
}
pub(super) async fn roots(
    services: &GrpcServices,
    request: Request<wire::BrowseActivityRootsRequest>,
) -> Result<Response<wire::BrowseActivityRootsResponse>, Status> {
    let actor = services
        .principal(&request, None, PrivateAudience::Reef)
        .await?;
    let input = request.into_inner();
    let window = if let Some(records) = input.retained {
        if !input.before.is_empty() || !input.after.is_empty() || !input.anchor.is_empty() {
            return Err(Status::invalid_argument("ambiguous activity window"));
        }
        if records.names.len() > usize::from(limit(input.page_size)?) {
            return Err(Status::invalid_argument(
                "too many retained activity records",
            ));
        }
        coral_task::ActivityWindow::Records(
            records
                .names
                .iter()
                .map(|name| root_identity(name))
                .collect::<Result<_, _>>()?,
        )
    } else {
        match (
            input.before.is_empty(),
            input.after.is_empty(),
            input.anchor.is_empty(),
        ) {
            (true, true, true) => coral_task::ActivityWindow::Edge,
            (false, true, true) => {
                coral_task::ActivityWindow::Before(root_identity(&input.before)?)
            }
            (true, false, true) => coral_task::ActivityWindow::After(root_identity(&input.after)?),
            (true, true, false) => coral_task::ActivityWindow::At(root_identity(&input.anchor)?),
            _ => return Err(Status::invalid_argument("ambiguous activity window")),
        }
    };
    let query = intent_query(&input.query)?;
    let page = services
        .tasks
        .activity_roots(
            &services.runs,
            &actor,
            &scope(&input.scope.unwrap_or_default())?,
            &window,
            limit(input.page_size)?,
            query.as_ref(),
        )
        .await
        .map_err(error)?;
    Ok(Response::new(wire::BrowseActivityRootsResponse {
        roots: page
            .rows
            .iter()
            .map(|row| wire::ActivityRoot {
                record: Some(match row {
                    coral_task::ActivityRoot::Task(value) => {
                        wire::activity_root::Record::Task(task(value))
                    }
                    coral_task::ActivityRoot::Run(value) => {
                        wire::activity_root::Record::Run(execution::run(value))
                    }
                }),
            })
            .collect(),
        has_before: page.has_before,
        has_after: page.has_after,
        anchor_unavailable: page.anchor_unavailable,
    }))
}
fn entry_anchor(
    name: &str,
    workspace: &coral_workspace::WorkspaceId,
    anchor: &str,
) -> Result<coral_run::RunEntryAnchor, Status> {
    if anchor == format!("{name}/final") {
        return Ok(coral_run::RunEntryAnchor::Final);
    }
    if let Some(sequence) = anchor.strip_prefix(&format!("{name}/events/")) {
        return Ok(coral_run::RunEntryAnchor::Event(sequence.parse().map_err(
            |_invalid| Status::invalid_argument("invalid Event identity"),
        )?));
    }
    let (owner, id) = resource(anchor, "invocations")?;
    if &owner != workspace {
        return Err(Status::invalid_argument("Invocation Workspace mismatch"));
    }
    Ok(coral_run::RunEntryAnchor::Invocation(
        coral_run::InvocationId::parse(id).map_err(execution::run_error)?,
    ))
}
pub(super) async fn entries(
    services: &GrpcServices,
    request: Request<wire::BrowseRunEntriesRequest>,
) -> Result<Response<wire::BrowseRunEntriesResponse>, Status> {
    let actor = services
        .principal(&request, None, PrivateAudience::Reef)
        .await?;
    let input = request.into_inner();
    let (workspace, id) = resource(&input.name, "runs")?;
    let id = coral_run::RunId::parse(id).map_err(execution::run_error)?;
    let selection = input.scope.as_ref().map(scope).transpose()?;
    let window = if let Some(viewport) = input.viewport {
        if input.after != 0 || !input.anchor.is_empty() {
            return Err(Status::invalid_argument("ambiguous execution window"));
        }
        coral_run::RunEntryWindow::Viewport {
            offset: viewport.offset,
            anchor: if viewport.anchor.is_empty() {
                None
            } else {
                Some(entry_anchor(&input.name, &workspace, &viewport.anchor)?)
            },
            preceding: u16::try_from(viewport.preceding).map_err(|_overflow| {
                Status::invalid_argument("invalid viewport predecessor budget")
            })?,
        }
    } else if input.anchor.is_empty() {
        coral_run::RunEntryWindow::After(input.after)
    } else {
        if input.after != 0 {
            return Err(Status::invalid_argument("ambiguous execution window"));
        }
        coral_run::RunEntryWindow::from(&entry_anchor(&input.name, &workspace, &input.anchor)?)
    };
    let history = services
        .tasks
        .activity_run_entries(
            &services.runs,
            &actor,
            &workspace,
            &id,
            &window,
            limit(input.page_size)?,
            selection.as_ref(),
        )
        .await
        .map_err(error)?;
    let page = history.execution;
    let mut run = execution::run(&page.run);
    run.task = execution::task_reference(history.submission.as_ref());
    let submission = history.submission.as_ref().map(|value| {
        let mut summary = execution::submission(value);
        summary.position = history.submission_position;
        summary
    });
    Ok(Response::new(wire::BrowseRunEntriesResponse {
        offset: page.offset,
        anchor_unavailable: page.anchor_unavailable,
        total: page.total,
        run: Some(run),
        submission,
        entries: page
            .entries
            .iter()
            .map(|entry| wire::ActivityRunEntry {
                record: Some(match entry {
                    coral_run::RunEntry::Invocation(value) => {
                        wire::activity_run_entry::Record::Invocation(execution::invocation(value))
                    }
                    coral_run::RunEntry::Event {
                        sequence,
                        kind,
                        occurred_at,
                        context_impact_bytes,
                        source_location,
                    } => wire::activity_run_entry::Record::Event(wire::ActivityEventSummary {
                        context_impact_bytes: Some(*context_impact_bytes),
                        sequence: *sequence,
                        kind: kind.clone(),
                        occurred_at_unix_nanos: *occurred_at,
                        source_location: source_location
                            .as_ref()
                            .map(|location| execution::source_location(&workspace, location)),
                    }),
                }),
            })
            .collect(),
        next_after: page.next_after,
        has_more: page.has_more,
        has_final_value: page.has_final_value,
    }))
}
pub(super) async fn event(
    services: &GrpcServices,
    request: Request<wire::InspectRunEventRequest>,
) -> Result<Response<wire::InspectRunEventResponse>, Status> {
    let actor = services
        .principal(&request, None, PrivateAudience::Reef)
        .await?;
    let input = request.into_inner();
    let (workspace, id) = resource(&input.run, "runs")?;
    let history = services
        .tasks
        .inspect_run_event(
            &services.runs,
            &actor,
            &workspace,
            &coral_run::RunId::parse(id).map_err(execution::run_error)?,
            input.sequence,
        )
        .await
        .map_err(error)?;
    let mut run = execution::run(&history.execution.run);
    run.task = execution::task_reference(history.submission.as_ref());
    Ok(Response::new(wire::InspectRunEventResponse {
        context_impact_bytes: Some(history.execution.context_impact_bytes),
        event: Some(execution::event(&workspace, &history.execution.event)),
        run: Some(run),
    }))
}
