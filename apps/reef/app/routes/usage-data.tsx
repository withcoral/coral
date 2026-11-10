import { data, useNavigation, useSubmit } from 'react-router'
import type { Route } from './+types/usage-data'
import {
  type UsageData,
  UsageHostKind,
  UsageSendBlock,
} from '@/generated/coral/management/v1/usage_data_pb'
import {
  actionFailure,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { isoTime } from '@/lib/management-views.server'
import type { SendBlock, UsageDataView } from '@/views/usage-data/types'
import { UsageDataPage } from '@/views/usage-data/usage-data-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ context }: Route.LoaderArgs) {
  try {
    const usage = await management(context).usageData.getUsageData({})
    return data({ view: usageView(usage), form: formContext(context) }, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const sharing = field(form, 'sharing')
  if (sharing !== 'on' && sharing !== 'off')
    throw new Response('Invalid form', { status: 400, headers: privateHeaders })
  try {
    await management(context).usageData.setUsageDataSharing({ sharing: sharing === 'on' })
    return data({ error: null }, { headers: privateHeaders })
  } catch (error) {
    return actionFailure(error, 'Coral could not change usage data sharing. ')
  }
}

/** Coral's answer as the page shows it. The report JSON passes through unchanged. */
function usageView(usage: UsageData): UsageDataView {
  const block = sendBlock(usage.block)
  return {
    choice: usage.sharing ? 'on' : 'off',
    canChange: usage.canChange,
    ...(block && { block }),
    kind: usage.kind === UsageHostKind.DESKTOP ? 'desktop' : 'server',
    next: usage.nextReportJson,
    ...(usage.lastSent && {
      lastSent: { at: isoTime(usage.lastSent.sentTime), json: usage.lastSent.json },
    }),
  }
}

/** A reason this Reef does not know still stops sending. */
function sendBlock(block: UsageSendBlock): SendBlock | undefined {
  switch (block) {
    case UsageSendBlock.NONE:
      return undefined
    case UsageSendBlock.DO_NOT_TRACK:
      return { kind: 'environment', variable: 'DO_NOT_TRACK' }
    case UsageSendBlock.CORAL_DISABLE_USAGE_DATA:
      return { kind: 'environment', variable: 'CORAL_DISABLE_USAGE_DATA' }
    case UsageSendBlock.SOURCE_BUILD:
      return { kind: 'build' }
    default:
      return { kind: 'unknown' }
  }
}

export default function UsageDataRoute({ loaderData, actionData }: Route.ComponentProps) {
  const submit = useSubmit()
  const navigation = useNavigation()
  // While the change is on its way, show the switch where the owner put it.
  const pending = navigation.formData?.get('sharing')
  const view: UsageDataView =
    pending === 'on' || pending === 'off'
      ? { ...loaderData.view, choice: pending }
      : loaderData.view
  return (
    <UsageDataPage
      view={view}
      error={navigation.state === 'idle' ? (actionData?.error ?? undefined) : undefined}
      onChoose={(share) =>
        submit({ csrf: loaderData.form.csrf, sharing: share ? 'on' : 'off' }, { method: 'post' })
      }
    />
  )
}
