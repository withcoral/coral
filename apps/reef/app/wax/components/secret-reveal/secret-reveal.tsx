import { useState, type ReactNode } from 'react'

import { Banner } from '@/wax/components/banner'
import { Button } from '@/wax/components'
import { Container } from '@/wax/components/card/container'
import { Typography } from '@/wax/components/typography'

import * as styles from './secret-reveal.css'

export interface SecretRevealProps {
  title: string
  /** Why the value cannot be shown again, and what to do if it is lost. */
  warning: string
  secret: string
  copyLabel: string
  dismissLabel: string
  /** Where to go after saving the secret; rendered beside Copy. */
  actions?: ReactNode
  /** Marks the secret element for recorded scenarios; never for production code. */
  testId?: string
}

/**
 * A value the host shows once and never again. It stays until the person
 * dismisses it, so a copy that failed can be tried again.
 */
export function SecretReveal({
  title,
  warning,
  secret,
  copyLabel,
  dismissLabel,
  actions,
  testId,
}: SecretRevealProps) {
  const [dismissed, setDismissed] = useState(false)
  if (dismissed) return null
  return (
    <Container as="section" className={styles.root} aria-label={title}>
      <div className={styles.intro}>
        <Typography.HeadingSmall as="h2">{title}</Typography.HeadingSmall>
        <Banner variant="warning">{warning}</Banner>
      </div>
      <Typography.Code as="code" className={styles.secret} data-testid={testId}>
        {secret}
      </Typography.Code>
      <div className={styles.actions}>
        <Button.CopyButton ariaLabel={copyLabel} textToCopy={secret}>
          {copyLabel}
        </Button.CopyButton>
        {actions}
        <Button.TextButton type="button" variant="secondary" onClick={() => setDismissed(true)}>
          {dismissLabel}
        </Button.TextButton>
      </div>
    </Container>
  )
}
