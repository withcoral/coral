import classNames from 'classnames'
import type { ReactNode } from 'react'

import { Logo, type LogoIcons } from '@/wax/components/logo'

import * as styles from './tool-name.css'

export interface ToolNameProps {
  /** The tool's name, or a link that holds it. */
  children: ReactNode
  /** The provider that owns the tool, shown by its logo. Leave it out where the context
   * already names the provider, such as under its heading. */
  provider?: { name: string; icons?: LogoIcons | null }
  /** `small` sits in a line of metadata; `default` stands in a table cell. */
  size?: 'default' | 'small'
  /** The full name, shown on hover when the name is cut short. */
  title?: string
  className?: string
}

/**
 * A tool's name, in the same face wherever Reef lists tools: Providers, Activity and
 * Approvals. It is not code, so it takes no chip. The provider's logo, if given, comes
 * first and names the provider for a screen reader.
 */
export function ToolName({
  children,
  provider,
  size = 'default',
  title,
  className,
}: ToolNameProps) {
  return (
    <span className={classNames(styles.root, className)} title={title}>
      {provider && (
        <span role="img" aria-label={provider.name} className={styles.logo}>
          <Logo name={provider.name} icons={provider.icons} size="18" />
        </span>
      )}
      <span className={styles.names[size]}>{children}</span>
    </span>
  )
}
