import type { ReactNode } from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'
import type { IconName } from '@/wax/components/icon'
import { Tooltip, type TooltipProps } from '@/wax/components/tooltip'

import { Container } from './container'
import { Icon } from './icon'
import { useCopy } from './use-copy'

interface CopyButtonProps {
  ariaLabel?: string
  children?: ReactNode
  className?: string
  disabled?: boolean
  /** What the failure toast tells the reader to do. */
  failureDescription?: string
  iconName?: IconName
  onCopy?: () => void
  size?: ButtonSize
  textToCopy: string
  /** Wraps the button in a tooltip with this text. */
  tooltipText?: string
  tooltipSide?: TooltipProps['side']
  variant?: ButtonVariant
}

/**
 * A button that copies text to the clipboard and shows feedback.
 */
export function CopyButton({
  ariaLabel = 'Copy to clipboard',
  children,
  className,
  disabled = false,
  failureDescription = 'Select and copy the text manually.',
  iconName = 'Copy',
  onCopy,
  size = '32',
  textToCopy,
  tooltipText,
  tooltipSide,
  variant = 'secondary',
}: CopyButtonProps) {
  const [copied, copy] = useCopy({ failureDescription, onCopy })

  const button = (
    <Container
      type="button"
      ariaLabel={copied ? `${ariaLabel}: copied` : ariaLabel}
      className={className}
      disabled={disabled}
      onClick={() => void copy(textToCopy)}
      size={size}
      variant={variant}
    >
      {/* Keep the label stable so feedback does not resize the button. */}
      <Icon name={copied ? 'Check' : iconName} />
      {children}
    </Container>
  )
  if (!tooltipText) return button
  return (
    <Tooltip content={tooltipText} side={tooltipSide}>
      {button}
    </Tooltip>
  )
}
