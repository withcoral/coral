import type { ReactNode } from 'react'
import { EmptyPage } from '@/components/empty-page'
import type { AttemptEnd } from '@/lib/provider-publication-attempt/model'
import type { IconName } from '@/wax/components/icon'
import { Typography } from '@/wax/components'
import * as styles from './providers.css'

const ENDS: Record<AttemptEnd, { icon: IconName; title: string; description: string }> = {
  expired: {
    icon: 'Clock',
    title: 'This setup has expired',
    description: 'A setup stays open for one hour. Start again to continue.',
  },
  canceled: {
    icon: 'CircleX',
    title: 'This setup is canceled',
    description: 'Start again to continue.',
  },
  consumed: {
    icon: 'CircleCheck',
    title: 'This setup is complete',
    description: 'Start again to read the source again.',
  },
}

/**
 * A Setup that can go no further, in place of its step. The icon and title
 * say why; the actions start a new Setup from the same source or leave.
 */
export function AttemptEnded({
  ended,
  description,
  error,
  children,
}: {
  ended: AttemptEnd
  /** Replaces the default line, when starting again needs something more from you. */
  description?: string
  /** Why the last try to start again failed. */
  error?: string | null
  /** The actions: leave first, then start again. */
  children: ReactNode
}) {
  const end = ENDS[ended]
  return (
    <EmptyPage
      iconName={end.icon}
      title={end.title}
      description={description ?? end.description}
      action={
        <div className={styles.emptyActions}>
          {error && (
            <Typography.Body as="p" role="alert">
              {error}
            </Typography.Body>
          )}
          <div className={styles.endedActions}>{children}</div>
        </div>
      }
    />
  )
}
