import { Popover } from '@base-ui/react/popover'
import { Tooltip as BaseTooltip } from '@base-ui/react/tooltip'
import classNames from 'classnames'
import { useState } from 'react'

import { useComposedRefs } from '@/utils/compose-refs'
import { useTruncationDetection } from '@/utils/use-truncation-detection'

import * as styles from './tooltip.css'

const DEFAULT_MAX_WIDTH = 300

export interface TooltipProps {
  children: React.ReactElement<Record<string, unknown>>
  className?: string
  content: React.ReactNode
  delay?: number
  maxWidth?: number
  /**
   * Also opens on a tap, a click or Enter, as a popover. Base UI opens no
   * tooltip on touch, so use it for a tip that a touch user needs too, such as
   * why a control is disabled. The trigger must not be a button.
   */
  openOnTap?: boolean
  showOnlyWhenTruncated?: boolean
  side?: 'bottom' | 'left' | 'right' | 'top'
  sideOffset?: number
}

export function Tooltip({
  children,
  className,
  content,
  delay = 400,
  maxWidth = DEFAULT_MAX_WIDTH,
  openOnTap = false,
  showOnlyWhenTruncated = false,
  side = 'top',
  sideOffset = 8,
}: TooltipProps) {
  if (openOnTap)
    return (
      <Popover.Root>
        <Popover.Trigger
          openOnHover
          delay={delay}
          nativeButton={false}
          render={children as React.ReactElement<Record<string, unknown>>}
        />
        <Popover.Portal>
          <Popover.Positioner className={styles.positioner} side={side} sideOffset={sideOffset}>
            <Popover.Popup
              className={classNames(styles.popup, className)}
              style={{ maxWidth: maxWidth }}
            >
              <Popover.Arrow className={styles.arrow} />
              {content}
            </Popover.Popup>
          </Popover.Positioner>
        </Popover.Portal>
      </Popover.Root>
    )
  return (
    <HoverTooltip
      className={className}
      content={content}
      delay={delay}
      maxWidth={maxWidth}
      showOnlyWhenTruncated={showOnlyWhenTruncated}
      side={side}
      sideOffset={sideOffset}
    >
      {children}
    </HoverTooltip>
  )
}

/** The tooltip that opens on hover and focus only. */
function HoverTooltip({
  children,
  className,
  content,
  delay,
  maxWidth,
  showOnlyWhenTruncated,
  side,
  sideOffset,
}: Required<Omit<TooltipProps, 'className' | 'openOnTap'>> & { className?: string }) {
  const [trigger, setTrigger] = useState<HTMLElement | null>(null)
  const isTruncated = useTruncationDetection(trigger, showOnlyWhenTruncated)
  const disabled = showOnlyWhenTruncated && !isTruncated

  const composedRef = useComposedRefs(
    showOnlyWhenTruncated ? setTrigger : undefined,
    (children as React.ReactElement<{ ref?: React.Ref<HTMLElement> }>).props?.ref,
  )

  return (
    <BaseTooltip.Provider delay={delay}>
      <BaseTooltip.Root disabled={disabled}>
        <BaseTooltip.Trigger
          ref={composedRef}
          render={children as React.ReactElement<Record<string, unknown>>}
        />
        <BaseTooltip.Portal>
          <BaseTooltip.Positioner className={styles.positioner} side={side} sideOffset={sideOffset}>
            <BaseTooltip.Popup
              className={classNames(styles.popup, className)}
              style={{ maxWidth: maxWidth }}
            >
              <BaseTooltip.Arrow className={styles.arrow} />
              {content}
            </BaseTooltip.Popup>
          </BaseTooltip.Positioner>
        </BaseTooltip.Portal>
      </BaseTooltip.Root>
    </BaseTooltip.Provider>
  )
}
