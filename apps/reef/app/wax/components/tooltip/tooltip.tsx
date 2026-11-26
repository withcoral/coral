import { Popover } from '@base-ui/react/popover'
import { Tooltip as BaseTooltip } from '@base-ui/react/tooltip'
import classNames from 'classnames'
import { useState } from 'react'

import { useComposedRefs } from '../../utils/compose-refs'
import { useTruncationDetection } from '../../utils/use-truncation-detection'

import * as styles from './tooltip.css'

const DEFAULT_MAX_WIDTH = 300

interface TooltipBaseProps {
  children: React.ReactElement<Record<string, unknown>>
  className?: string
  content: React.ReactNode
  delay?: number
  maxWidth?: number
  side?: 'bottom' | 'left' | 'right' | 'top'
  sideOffset?: number
}

export type TooltipProps = TooltipBaseProps &
  (
    | { openOnTap?: false; showOnlyWhenTruncated?: boolean }
    | {
        /**
         * Also opens on a tap, a click, Enter or keyboard focus, as a popover. Base UI
         * opens no tooltip on touch, so use it for a tip that a touch user needs too,
         * such as why a control is disabled. The trigger must not be a button. A tip
         * that a touch user needs is never only for truncated text.
         */
        openOnTap: true
        showOnlyWhenTruncated?: never
      }
  )

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
      <TapTooltip
        className={className}
        content={content}
        delay={delay}
        maxWidth={maxWidth}
        side={side}
        sideOffset={sideOffset}
      >
        {children}
      </TapTooltip>
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

/**
 * The tooltip that is also a popover, so a tap opens it. Keyboard focus opens it too,
 * as it opens a tooltip, and focus stays on the trigger: the popover holds only text.
 */
function TapTooltip({
  children,
  className,
  content,
  delay,
  maxWidth,
  side,
  sideOffset,
}: Required<Omit<TooltipBaseProps, 'className'>> & { className?: string }) {
  const [open, setOpen] = useState(false)
  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger
        openOnHover
        delay={delay}
        nativeButton={false}
        onFocus={(event) => {
          if (event.currentTarget.matches(':focus-visible')) setOpen(true)
        }}
        onBlur={() => setOpen(false)}
        render={children}
      />
      <Popover.Portal>
        <Popover.Positioner className={styles.positioner} side={side} sideOffset={sideOffset}>
          <Popover.Popup
            className={classNames(styles.popup, className)}
            initialFocus={false}
            finalFocus={false}
            style={{ maxWidth: maxWidth }}
          >
            <Popover.Arrow className={styles.arrow} />
            {content}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
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
}: Required<Omit<TooltipBaseProps, 'className'> & { showOnlyWhenTruncated: boolean }> & {
  className?: string
}) {
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
