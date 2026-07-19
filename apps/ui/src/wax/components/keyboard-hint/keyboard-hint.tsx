import classNames from 'classnames'

import * as styles from './keyboard-hint.css'
import { formatKey, parseShortcut } from './utils'

export interface KeyboardHintProps {
  className?: string
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
