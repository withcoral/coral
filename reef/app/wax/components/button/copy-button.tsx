import { ReactNode, useCallback, useState } from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'
import type { IconName } from '@/wax/components/icon'

import { Container } from './container'
import { Icon } from './icon'

interface CopyButtonProps {
  children?: ReactNode
  className?: string
  disabled?: boolean
  iconName?: IconName
  onCopy?: () => void
  size?: ButtonSize
  textToCopy: string
  variant?: ButtonVariant
}

/**
 * A button that copies text to the clipboard and shows feedback.
 */
export function CopyButton({
  children,
  className,
  disabled = false,
  iconName = 'Copy',
  onCopy,
  size = '32',
  textToCopy,
  variant = 'secondary',
}: CopyButtonProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(textToCopy)
      setCopied(true)
      onCopy?.()
      setTimeout(() => setCopied(false), 3000)
    } catch (err) {
      console.error('Failed to copy:', err)
    }
  }, [textToCopy, onCopy])

  return (
    <Container
      className={className}
      disabled={disabled}
      onClick={handleCopy}
      size={size}
      variant={variant}
    >
      <Icon name={copied ? 'Check' : iconName} />
      {children}
    </Container>
  )
}
