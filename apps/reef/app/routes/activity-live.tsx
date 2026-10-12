import { useActivityReader } from '@/lib/activity-observation'
import { Activity } from '@/views/activity/activity'
import { ActionReviewSession } from '@/views/actions/review'
import { ActivityNavigationContext } from '@/views/activity/navigation'
import type { ActivityPage, ActivityViewProps } from '@/views/activity/types'

/** The route only connects the Activity reader to presentation. */
export function LiveActivity({
  initial,
  connect,
  more,
}: {
  initial: ActivityPage
  connect?: ActivityViewProps['connect']
  more?: ActivityViewProps['more']
}) {
  const { navigation, ...activity } = useActivityReader(initial)
  return (
    <ActionReviewSession
      request={
        activity.page.detail?.kind === 'run' || activity.page.detail?.kind === 'invocation'
          ? activity.page.detail.request
          : null
      }
    >
      <ActivityNavigationContext value={navigation}>
        <Activity {...activity} connect={connect} more={more} />
      </ActivityNavigationContext>
    </ActionReviewSession>
  )
}
