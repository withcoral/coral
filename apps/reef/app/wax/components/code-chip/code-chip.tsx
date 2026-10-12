import classNames from 'classnames'
import type { ReactNode } from 'react'

import * as styles from './code-chip.css'

/** One short identifier in code, on its own tinted chip: a call path, a program name. */
export function CodeChip({ children, className }: { children: ReactNode; className?: string }) {
  return <code className={classNames(styles.chip, className)}>{children}</code>
}
