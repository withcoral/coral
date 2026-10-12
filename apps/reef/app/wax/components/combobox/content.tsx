import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'
import { ComponentProps, RefObject } from 'react'

import { useAnchorContext } from './anchor-context'
import * as styles from './combobox.css'

export interface ContentProps {
  align?: 'center' | 'end' | 'start'
  alignOffset?: number
  anchor?: ComponentProps<typeof BaseCombobox.Positioner>['anchor']
  children: React.ReactNode
  className?: string
  container?: HTMLElement | null | RefObject<HTMLElement | null>
  side?: 'bottom' | 'left' | 'right' | 'top'
  sideOffset?: number
}

export function Content({
  align = 'start',
  alignOffset = 0,
  anchor,
  children,
  className,
  container,
  side = 'bottom',
  sideOffset = 4,
}: ContentProps) {
  const anchorContext = useAnchorContext()

  return (
    <BaseCombobox.Portal container={container}>
      <BaseCombobox.Positioner
        align={align}
        alignOffset={alignOffset}
        anchor={anchor ?? anchorContext?.inputGroupAnchor ?? undefined}
        className={styles.positioner}
        side={side}
        sideOffset={sideOffset}
      >
        <BaseCombobox.Popup className={classNames(styles.popup, className)}>
          {children}
        </BaseCombobox.Popup>
      </BaseCombobox.Positioner>
    </BaseCombobox.Portal>
  )
}
