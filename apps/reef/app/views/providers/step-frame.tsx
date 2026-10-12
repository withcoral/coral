import { createContext, useContext, type ReactNode } from 'react'
import { Typography } from '@/wax/components'
import * as styles from './providers.css'

/** Holds when the step is the page: its title and actions head the content. */
export const StepPage = createContext(false)

/**
 * A step's content and the actions that move past it. Under a StepPage a header
 * row puts the title first and the actions at the content's edge; elsewhere the
 * actions follow the content.
 */
export function StepFrame({
  title,
  meta,
  actions,
  children,
}: {
  title?: string
  /** A quiet line beside the title. */
  meta?: ReactNode
  actions?: ReactNode
  children: ReactNode
}) {
  const headed = useContext(StepPage)
  const bar = actions ? <div className={styles.stepActions}>{actions}</div> : null
  if (!headed)
    return (
      <div className={styles.stack}>
        {children}
        {bar}
      </div>
    )
  return (
    <div className={styles.stack}>
      <div className={styles.stepHeader}>
        <div className={styles.stepTitle}>
          {title && <Typography.HeadingSmall as="h2">{title}</Typography.HeadingSmall>}
          {meta}
        </div>
        {bar}
      </div>
      {children}
    </div>
  )
}
