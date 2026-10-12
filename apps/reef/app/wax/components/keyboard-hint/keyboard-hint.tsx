import classNames from 'classnames'

import * as styles from './keyboard-hint.css'
import { formatKey, parseShortcut } from './utils'

export interface KeyboardHintProps {
  className?: string
  /**
   * The keyboard shortcut to display.
   * Can be a string like "mod+k" or "mod [", or an array of keys like ["mod", "k"].
   * Modifier keys (mod, alt, shift, ctrl) are converted to platform-specific symbols.
   */
  shortcut: string | string[]
}

export function KeyboardHint({ className, shortcut }: KeyboardHintProps) {
  const keys = Array.isArray(shortcut) ? shortcut : parseShortcut(shortcut)

  return (
    <span className={classNames(styles.container, className)}>
      {keys.map((key, index) => (
        <span key={`${key}-${index}`}>{formatKey(key)}</span>
      ))}
    </span>
  )
}
