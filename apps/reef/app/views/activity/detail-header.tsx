import type { ReactNode } from 'react'
import { Typography } from '@/wax/components'
import type { IconName, IconPlateTone } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import { PanelHeader } from './panel'

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
  kind,
  sticky,
  heading,
  showStatus = true,
  meta,
}: {
  title: string
  /** The exact retained state; it selects the tone and icon. */
  status: string
  /** The pill's text, when it says more than the state. */
  label?: string
  /** The record kind, named under the title. */
  kind?: string
  sticky?: boolean
  heading?: 'h2' | 'h3'
  /** False when the title already says what the record is, so a badge would only repeat it. */
  showStatus?: boolean
  /** More about the record under its kind, such as a live deadline. */
  meta?: ReactNode
}) {
  const tone = tones[status] ?? 'gray'
  return (
    <PanelHeader
      icon={icons[tone]}
      tone={tone}
      title={title}
      heading={heading}
      sticky={sticky}
      details={
        <>
          {kind && <Typography.BodySmall variant="tertiary">{kind}</Typography.BodySmall>}
          {meta}
        </>
      }
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
