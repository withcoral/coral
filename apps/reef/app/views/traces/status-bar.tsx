import { Typography } from '@/wax/components/typography'

import * as s from './traces.css'

export function StatusBar({
  connected,
  count,
  endpointLabel,
  totalCount = count,
}: {
  connected: boolean
  count: number
  endpointLabel: string
  totalCount?: number
}) {
  return (
    <div className={s.statusBar}>
      <div className={s.statusLeft}>
        <span className={s.statusBarDot} data-state={connected ? 'connected' : 'disconnected'} />
        <Typography.BodySmall as="span" variant="tertiary">
          {connected ? 'Connected' : 'Disconnected'}
        </Typography.BodySmall>
        <span className={s.statusSep} />
        <Typography.BodySmall as="span" variant="tertiary">
          {endpointLabel}
        </Typography.BodySmall>
      </div>
      <div className={s.statusRight}>
        <Typography.BodySmall as="span" variant="tertiary">
          {count === totalCount
            ? `${count} ${count === 1 ? 'operation' : 'operations'}`
            : `${count} of ${totalCount} operations`}
        </Typography.BodySmall>
        <span className={s.statusSep} />
        <Typography.BodySmall as="span" variant="tertiary">
          Coral
        </Typography.BodySmall>
      </div>
    </div>
  )
}
