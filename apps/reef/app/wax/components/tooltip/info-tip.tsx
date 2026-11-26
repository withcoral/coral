import { Popover } from '@base-ui/react/popover'

import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import type { TooltipProps } from './tooltip'
import * as styles from './tooltip.css'

export interface InfoTipProps {
  /** The explanation. */
  children: string
  /** Text before the icon. Without it, only the icon shows. */
  label?: string
  /**
   * The name screen readers announce for the icon and the explanation. Give
   * an icon-only tip one, such as "About members". Defaults to the label.
   */
  ariaLabel?: string
  side?: TooltipProps['side']
}

/**
 * An info icon that shows an explanation on hover, tap, or keypress, with an
 * optional label before it. Use it instead of a loose line of help text.
 *
 * It is a popover, not a tooltip: Base UI disables tooltips on touch devices,
 * and touch users must also read the explanation.
 */
export function InfoTip({ children, label, ariaLabel, side = 'top' }: InfoTipProps) {
  const name = ariaLabel || label || 'More information'
  const tip = (
    <Popover.Root>
      <Popover.Trigger openOnHover delay={400} className={styles.infoTip} aria-label={name}>
        <Icon name="Info" size="14" color="secondary" />
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner className={styles.positioner} side={side} sideOffset={8}>
          <Popover.Popup className={styles.popup} style={{ maxWidth: 300 }} aria-label={name}>
            <Popover.Arrow className={styles.arrow} />
            {children}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
  if (!label) return tip
  return (
    <span className={styles.infoTipLabel}>
      <Typography.BodySmallStrong as="span">{label}</Typography.BodySmallStrong>
      {tip}
    </span>
  )
}
