import React from 'react'

import type { IconName } from '@/wax/components/icon'

import { Tooltip } from '@/wax/components/tooltip'
import type { TooltipProps } from '@/wax/components/tooltip'

import type { ButtonProps } from './container'
import { Container } from './container'
import { Icon } from './icon'

type IconButtonProps = IconButtonWithAriaLabel | IconButtonWithTooltip

interface IconButtonPropsBase extends Omit<ButtonProps, 'ariaLabel' | 'children'> {
  name: IconName
  ref?: React.Ref<HTMLButtonElement>
  tooltipSide?: TooltipProps['side']
}

interface IconButtonWithAriaLabel extends IconButtonPropsBase {
  /**
   * Required when tooltipText is not provided (e.g., when wrapped in
   * KeyboardShortcut or another component that provides its own tooltip).
   */
  ariaLabel: string
  tooltipText?: never
}

interface IconButtonWithTooltip extends IconButtonPropsBase {
  /** Overrides tooltipText for the aria-label if both are provided. */
  ariaLabel?: string
  /**
   * Text for the tooltip and aria-label. When provided, the button is
   * wrapped in a Tooltip.
   */
  tooltipText: string
}

/**
 * Convenience component for a button containing only an icon.
 */
export function IconButton({ ariaLabel, name, ref, tooltipSide, tooltipText, ...rest }: IconButtonProps) {
  const button = (
    <Container ariaLabel={ariaLabel ?? tooltipText} ref={ref} {...rest}>
      <Icon name={name} />
    </Container>
  )

  if (!tooltipText) {
    return button
  }

  return (
    <Tooltip content={tooltipText} side={tooltipSide}>
      {button}
    </Tooltip>
  )
}
