import type { ReactNode } from 'react'

import { Dialog } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import { Markdown } from '@/components/markdown'
import { formatSourceName, ProviderLogo } from '@/components/sources'
import type { CatalogSourceInputSpec, SourceOriginLabel } from '@/lib/sources'
import { toSentenceCase } from '@/utils/to-sentence-case'

import * as styles from './source-presentation.css'

export function SourceHeader({
  description,
  name,
  origin,
  version,
}: {
  description: string
  name: string
  origin: SourceOriginLabel
  version?: string
}) {
  const originLabel = sourceOriginLabel(origin)

  return (
    <div className={styles.header}>
      <ProviderLogo name={name} size="large" />
      <div className={styles.headerText}>
        <Dialog.Title className={styles.headerTitleRow}>
          <span className={styles.headerIdentity}>
            <Typography.HeadingMedium as="span" className={styles.headerTitle}>
              {formatSourceName(name)}
            </Typography.HeadingMedium>
            {version ? (
              <Typography.BodySmall as="span" variant="tertiary">
                {version}
              </Typography.BodySmall>
            ) : null}
          </span>
          {originLabel ? <Pill color="graySubtle">{originLabel}</Pill> : null}
        </Dialog.Title>
        <Dialog.Description render={<div />}>
          <Markdown>{description}</Markdown>
        </Dialog.Description>
      </div>
    </div>
  )
}

export function SourceInputField({
  children,
  htmlFor,
  input,
  showHint = true,
  showLabel = true,
}: {
  children: ReactNode
  htmlFor?: string
  input: CatalogSourceInputSpec
  showHint?: boolean
  showLabel?: boolean
}) {
  return (
    <SourceField
      hint={showHint ? input.hint : undefined}
      htmlFor={htmlFor}
      label={showLabel ? formatFieldName(input.key) : undefined}
    >
      {children}
    </SourceField>
  )
}

export function SourceNoConfiguration() {
  return (
    <div className={styles.noConfiguration}>
      <Typography.BodyStrong variant="primary">No setup required</Typography.BodyStrong>
      <Typography.BodySmall variant="secondary">
        This source doesn’t need credentials or additional settings.
      </Typography.BodySmall>
    </div>
  )
}

export function SourceField({
  children,
  hint,
  htmlFor,
  label,
}: {
  children: ReactNode
  hint?: string
  htmlFor?: string
  label?: string
}) {
  return (
    <div className={styles.fieldItem}>
      {label ? (
        htmlFor ? (
          <Typography.BodyStrong as="label" htmlFor={htmlFor} variant="primary">
            {label}
          </Typography.BodyStrong>
        ) : (
          <Typography.BodyStrong variant="primary">{label}</Typography.BodyStrong>
        )
      ) : null}
      {children}
      {hint ? <Markdown>{hint}</Markdown> : null}
    </div>
  )
}

export function formatFieldName(key: string): string {
  return toSentenceCase(key.replace(/_/g, ' '))
}

function sourceOriginLabel(origin: SourceOriginLabel): string | null {
  if (origin === 'bundled') return 'Core'
  if (origin === 'imported') return 'Imported'
  return null
}
