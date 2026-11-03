import type { ReactNode } from 'react'
import type { IconName, IconPlateTone } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import { PanelHeader } from './panel'
import { CallTitle, type CallProvider } from './call-tool'

const tones: Record<string, IconPlateTone> = {
  success: 'green',
  succeeded: 'green',
  connected: 'green',
  failure: 'red',
  failed: 'red',
  rejected: 'red',
  coral_failure: 'red',
  declined: 'red',
  expired: 'red',
  timed_out: 'red',
  pending: 'amber',
  unknown: 'amber',
  indeterminate: 'amber',
  interrupted: 'amber',
}
const icons: Record<IconPlateTone, IconName> = {
  green: 'CircleCheck',
  red: 'CircleX',
  amber: 'CircleAlert',
  gray: 'Circle',
}

/** A retained record's header: its exact state picks the icon and tone, never colour alone. */
export function DetailHeader({
  title,
  status,
  label = status.replaceAll('_', ' '),
  sticky,
  heading,
  showStatus = true,
  meta,
  provider,
}: {
  title: string
  /** The exact retained state; it selects the tone and icon. */
  status: string
  /** The pill's text, when it says more than the state. */
  label?: string
  sticky?: boolean
  heading?: 'h2' | 'h3'
  /** False when the title already says what the record is, so a badge would only repeat it. */
  showStatus?: boolean
  /** More about the record under its title, such as a live deadline. */
  meta?: ReactNode
  /** A call's provider. Its logo leads the title in place of the state icon. */
  provider?: CallProvider | null
}) {
  const tone = tones[status] ?? 'gray'
  return (
    <PanelHeader
      icon={provider ? undefined : icons[tone]}
      tone={tone}
      title={provider ? <CallTitle callPath={title} provider={provider} /> : title}
      heading={heading}
      sticky={sticky}
      details={meta}
      status={showStatus && <Pill color={tone}>{label}</Pill>}
    />
  )
}

/** Another record's state inside a panel, named for that record so it never reads as the
 * panel's own: "program run: failed". Text carries the meaning; the colour repeats it. */
export function StateBadge({ entity, state }: { entity: string; state: string }) {
  return (
    <Pill color={tones[state] ?? 'gray'} size="small">
      {`${entity}: ${state.replaceAll('_', ' ')}`}
    </Pill>
  )
}
