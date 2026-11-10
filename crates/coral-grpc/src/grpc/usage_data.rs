use super::workspaces::{annotate, timestamp};
use super::{GrpcServices, management as wire, now};
use tonic::{Request, Response, Status};

fn view(view: coral_usage::UsageDataView) -> wire::UsageData {
    use coral_usage::{BlockingVariable, SendBlock};
    wire::UsageData {
        sharing: view.sharing,
        can_change: view.can_change,
        block: match view.block {
            None => wire::UsageSendBlock::None,
            Some(SendBlock::Environment(BlockingVariable::DoNotTrack)) => {
                wire::UsageSendBlock::DoNotTrack
            }
            Some(SendBlock::Environment(BlockingVariable::CoralTelemetry)) => {
                wire::UsageSendBlock::CoralTelemetry
            }
            Some(SendBlock::Environment(BlockingVariable::Ci)) => wire::UsageSendBlock::Ci,
            Some(SendBlock::SourceBuild) => wire::UsageSendBlock::SourceBuild,
        }
        .into(),
        kind: match view.kind {
            coral_usage::HostKind::Desktop => wire::UsageHostKind::Desktop,
            coral_usage::HostKind::Server => wire::UsageHostKind::Server,
        }
        .into(),
        last_sent: view.last_sent.map(|sent| wire::SentUsageReport {
            sent_time: Some(timestamp(sent.sent_at_unix_nanos)),
            json: sent.json,
        }),
        next_report_json: view.next_report_json,
    }
}

fn error(error: &coral_usage::Error) -> Status {
    annotate(match error {
        coral_usage::Error::PermissionDenied => Status::permission_denied("permission denied"),
        coral_usage::Error::Database(_)
        | coral_usage::Error::CorruptData(_)
        | coral_usage::Error::Unavailable => Status::unavailable("usage data unavailable"),
    })
}

impl GrpcServices {
    fn usage(&self) -> Result<&coral_usage::UsageData, Status> {
        self.usage
            .as_ref()
            .ok_or_else(|| annotate(Status::unimplemented("usage data is not available")))
    }
}

#[tonic::async_trait]
impl wire::usage_data_service_server::UsageDataService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_usage_data")]
    async fn get_usage_data(
        &self,
        request: Request<wire::GetUsageDataRequest>,
    ) -> Result<Response<wire::UsageData>, Status> {
        let actor = self.management_user(&request).await?;
        let usage = self.usage()?;
        let data = usage
            .view(&actor, now()?)
            .await
            .map_err(|failure| error(&failure))?;
        Ok(Response::new(view(data)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.set_usage_data_sharing")]
    async fn set_usage_data_sharing(
        &self,
        request: Request<wire::SetUsageDataSharingRequest>,
    ) -> Result<Response<wire::UsageData>, Status> {
        let actor = self.management_user(&request).await?;
        let sharing = request.get_ref().sharing;
        let usage = self.usage()?;
        let data = usage
            .set_sharing(&actor, sharing, now()?)
            .await
            .map_err(|failure| error(&failure))?;
        Ok(Response::new(view(data)))
    }
}
