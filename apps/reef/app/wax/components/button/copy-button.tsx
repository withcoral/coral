import { ReactNode, useCallback, useState } from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'
import type { IconName } from '@/wax/components/icon'
import { addToast } from '@/wax/components/toast'
import { Tooltip, type TooltipProps } from '@/wax/components/tooltip'

import { Container } from './container'
import { Icon } from './icon'

interface CopyButtonProps {
  ariaLabel?: string
  children?: ReactNode
  className?: string
  disabled?: boolean
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
  iconName = 'Copy',
  onCopy,
  size = '32',
  textToCopy,
  tooltipText,
  tooltipSide,
  variant = 'secondary',
}: CopyButtonProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(textToCopy)
      setCopied(true)
      onCopy?.()
      setTimeout(() => setCopied(false), 3000)
    } catch {
      setCopied(false)
      addToast('error', {
        title: 'Copy failed',
        description: 'Select and copy the text manually.',
      })
    }
  }, [textToCopy, onCopy])

  const button = (
    <Container
      type="button"
      ariaLabel={copied ? `${ariaLabel}: copied` : ariaLabel}
      className={className}
      disabled={disabled}
      onClick={handleCopy}
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
