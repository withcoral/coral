import { useActivityReader } from '@/lib/activity-observation'
import { Activity } from '@/views/activity/activity'
import { ActionReviewSession } from '@/views/actions/review'
import { ActivityNavigationContext, leavesActivityReview } from '@/views/activity/navigation'
import { ActivityArchivedContext } from '@/views/activity/live'
import { ProviderMarksContext, type ProviderMark } from '@/views/activity/call-tool'
import type { ActivityPage, ActivityViewProps } from '@/views/activity/types'

/** The route only connects the Activity reader to presentation. */
export function LiveActivity({
  initial,
  connect,
  archived,
  more,
  providers = [],
}: {
  initial: ActivityPage
  connect?: ActivityViewProps['connect']
  /** Your role in the Workspace when it is archived. */
  archived?: 'owner' | 'member'
  more?: ActivityViewProps['more']
  /** The workspace's providers, which name each call by its provider's logo and tool. */
  providers?: readonly ProviderMark[]
}) {
  const { navigation, ...activity } = useActivityReader(initial)
  const request =
    activity.page.detail?.kind === 'run' || activity.page.detail?.kind === 'invocation'
      ? activity.page.detail.request
      : null
  return (
    <ActionReviewSession request={request} leaves={leavesActivityReview(request)}>
      <ActivityNavigationContext value={navigation}>
        <ActivityArchivedContext value={archived ?? null}>
          <ProviderMarksContext value={providers}>
            <Activity {...activity} connect={connect} more={more} />
          </ProviderMarksContext>
        </ActivityArchivedContext>
      </ActivityNavigationContext>
    </ActionReviewSession>
  )
}
