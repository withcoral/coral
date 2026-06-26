import classNames from 'classnames'
import { useState } from 'react'

import { IconButton } from '@/wax/components/button'

import * as styles from './secrets-input.css'

export interface SecretsInputProps {
  className?: string
  content: string
  secrets?: [number, number][]
}

export function SecretsInput({ className, content, secrets }: SecretsInputProps) {
  const [isRevealed, setIsRevealed] = useState(false)

  const displayContent = getMaskedContent(content, secrets, isRevealed)

  return (
    <div className={classNames(styles.container, className)}>
      <span className={styles.content}>{displayContent}</span>
      <IconButton
        name={isRevealed ? 'EyeOff' : 'Eye'}
        onClick={() => setIsRevealed(!isRevealed)}
        size="22"
        tooltipText={isRevealed ? 'Hide secret' : 'Reveal secret'}
        variant="bare"
      />
    </div>
  )
}

function getMaskedContent(
  content: string,
  secrets: [number, number][] | undefined,
  isRevealed: boolean,
): string {
  if (isRevealed) {
    return content
  }

  // If no secrets specified, mask the entire content
  if (!secrets || secrets.length === 0) {
    return '*'.repeat(content.length)
  }

  const chars = content.split('')

  for (const [start, end] of secrets) {
    for (let i = start; i < end && i < chars.length; i++) {
      chars[i] = '*'
    }
  }

  return chars.join('')
}
