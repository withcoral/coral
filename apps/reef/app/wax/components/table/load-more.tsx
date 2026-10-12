import { useEffect, useLayoutEffect, useRef, type ReactNode } from 'react'

import { animations } from '@/wax/animations'
import { TextButton } from '../button/text-button'
import { Icon } from '../icon'
import { Typography } from '../typography'
import { visuallyHidden } from '../visually-hidden.css'
import { Status } from './status'
import * as styles from './table.css'

export interface LoadMoreProps {
  /** The button's label, such as "Load older records". */
  children: ReactNode
  /** Read out while a load is in flight. Sighted readers see only a spinner. */
  loadingLabel: ReactNode
  onLoad: () => void
  loading?: boolean
  /** Load as soon as the row scrolls near the viewport, not only on a click. */
  automatic?: boolean
  /** Says why the last load failed. The button then retries. */
  error?: ReactNode
  /** The button's label while `error` shows, such as "Try again". */
  retryLabel: ReactNode
}

/** How far outside the scroll port the row still counts as near it. */
const NEAR = '160px'

/**
 * The row at an end of a `layout="scroll"` table that loads more rows. While it
 * waits, it is a button, so a keyboard or screen reader can load more too. With
 * `automatic`, it loads by itself while it is near the viewport: once when it
 * comes near, and again after each load that leaves it there. A load in flight
 * is a spinner, not a control; assistive technology reads its label.
 */
export function LoadMore({
  children,
  loadingLabel,
  onLoad,
  loading = false,
  automatic = false,
  error,
  retryLabel,
}: LoadMoreProps) {
  const row = useRef<HTMLDivElement>(null)
  const button = useRef<HTMLButtonElement>(null)
  const status = useRef<HTMLSpanElement>(null)
  const near = useRef(false)
  const load = useRef(onLoad)
  // Focus follows the row between its button and its status, so a keyboard
  // user who started a load does not fall back to the top of the page.
  const focused = useRef(false)
  const ready = automatic && !loading && !error
  useEffect(() => {
    load.current = onLoad
  })
  useLayoutEffect(() => {
    if (focused.current) (loading ? status : button).current?.focus({ preventScroll: true })
  }, [loading])
  useEffect(() => {
    const element = row.current
    if (!element || typeof IntersectionObserver === 'undefined') return
    const observer = new IntersectionObserver(
      ([entry]) => {
        near.current = Boolean(entry?.isIntersecting)
        if (near.current && ready) load.current()
      },
      { root: element.closest('[role=table]'), rootMargin: `${NEAR} 0px` },
    )
    observer.observe(element)
    return () => observer.disconnect()
  }, [ready])
  const track = {
    onFocus: () => {
      focused.current = true
    },
    onBlur: () => {
      focused.current = false
    },
  }
  return (
    <Status scope="group" ref={row}>
      {loading ? (
        <span ref={status} role="status" tabIndex={-1} className={styles.loadMoreStatus} {...track}>
          <Icon name="Loader" size="14" color="tertiary" className={animations.spinAnimation} />
          <span className={visuallyHidden}>{loadingLabel}</span>
        </span>
      ) : (
        <>
          {error && <Typography.BodySmall role="alert">{error} </Typography.BodySmall>}
          <TextButton ref={button} size="22" variant="link" onClick={onLoad} {...track}>
            {error ? retryLabel : children}
          </TextButton>
        </>
      )}
    </Status>
  )
}
