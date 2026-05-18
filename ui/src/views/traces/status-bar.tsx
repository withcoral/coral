import { Typography } from '@/wax/components/typography'

import * as s from '../traces-page.css'

export function StatusBar({ connected, count, totalCount = count }: { connected: boolean; count: number; totalCount?: number }) {
  const endpoint = typeof window === 'undefined' ? 'TraceService' : window.location.host
  return (
    <div className={s.statusBar}>
      <div className={s.statusLeft}>
        <span className={s.statusBarDot} data-state={connected ? 'connected' : 'disconnected'} />
        <Typography.BodySmall as="span" variant="tertiary">{connected ? 'Connected' : 'Disconnected'}</Typography.BodySmall>
        <span className={s.statusSep} />
        <Typography.BodySmall as="span" variant="tertiary">{endpoint}</Typography.BodySmall>
      </div>
      <div className={s.statusRight}>
        <Typography.BodySmall as="span" variant="tertiary">{count === totalCount ? `${count} ${count === 1 ? 'query' : 'queries'}` : `${count} of ${totalCount} queries`}</Typography.BodySmall>
        <span className={s.statusSep} />
        <Typography.BodySmall as="span" variant="tertiary">Coral</Typography.BodySmall>
      </div>
    </div>
  )
}
