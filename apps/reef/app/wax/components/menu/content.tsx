import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'
import { RefObject } from 'react'

import * as styles from './menu.css'

/**
 * A closing menu stays mounted while it fades out, and Base UI makes it `inert`.
 * `aria-hidden` says the same to tools that ignore `inert`, such as Testing Library,
 * so a query never finds an item that is about to unmount.
 */
export function hideWhileClosing(props: React.ComponentProps<'div'>, state: { open: boolean }) {
  return <div {...props} aria-hidden={state.open ? undefined : true} />
}

export interface ContentProps {
  align?: 'center' | 'end' | 'start'
  alignOffset?: number
  children: React.ReactNode
  className?: string
  container?: HTMLElement | null | RefObject<HTMLElement | null>
  side?: 'bottom' | 'left' | 'right' | 'top'
  sideOffset?: number
}

export function Content({
  align = 'start',
  alignOffset = 0,
  children,
  className,
  container,
  side = 'bottom',
  sideOffset = 4,
}: ContentProps) {
  return (
    <BaseMenu.Portal container={container}>
      <BaseMenu.Positioner
        align={align}
        alignOffset={alignOffset}
        className={styles.positioner}
        render={hideWhileClosing}
        side={side}
        sideOffset={sideOffset}
      >
        <BaseMenu.Popup className={classNames(styles.popup, className)}>{children}</BaseMenu.Popup>
      </BaseMenu.Positioner>
    </BaseMenu.Portal>
  )
}
