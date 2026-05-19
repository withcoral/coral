import { Tooltip as BaseTooltip } from '@base-ui-components/react/tooltip'
import classNames from 'classnames'
import { useRef } from 'react'

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
  showOnlyWhenTruncated = false,
  side = 'top',
  sideOffset = 8,
}: TooltipProps) {
  const triggerRef = useRef<HTMLElement>(null)
  const isTruncated = useTruncationDetection(triggerRef, showOnlyWhenTruncated)
  const disabled = showOnlyWhenTruncated && !isTruncated

  const composedRef = useComposedRefs(
    triggerRef,
    (children as React.ReactElement<{ ref?: React.Ref<HTMLElement> }>).props?.ref
  )

  return (
    <BaseTooltip.Provider delay={delay}>
      <BaseTooltip.Root disabled={disabled}>
        <BaseTooltip.Trigger ref={composedRef} render={children as React.ReactElement<Record<string, unknown>>} />
        <BaseTooltip.Portal>
          <BaseTooltip.Positioner className={styles.positioner} side={side} sideOffset={sideOffset}>
            <BaseTooltip.Popup className={classNames(styles.popup, className)} style={{ maxWidth: maxWidth }}>
              <BaseTooltip.Arrow className={styles.arrow} />
              {content}
            </BaseTooltip.Popup>
          </BaseTooltip.Positioner>
        </BaseTooltip.Portal>
      </BaseTooltip.Root>
    </BaseTooltip.Provider>
  )
}
