import type { ReactNode } from 'react'
import classNames from 'classnames'

import { Dialog } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import { Markdown } from '@/components/markdown'
import { formatSourceName, ProviderLogo } from '@/components/sources'
import type { CatalogSourceInputSpec, SourceOriginLabel } from '@/lib/sources'
import { toSentenceCase } from '@/utils/to-sentence-case'

import * as styles from './source-presentation.css'

export function SourceHeader({
  className,
  description,
  leading,
  pill,
  title,
}: {
  className?: string
  description?: ReactNode
  leading?: ReactNode
  pill?: ReactNode
  title: ReactNode
}) {
  return (
    <div className={classNames(styles.header, className)}>
      {leading}
      <div className={styles.headerText}>
        <Dialog.Title className={styles.headerTitleRow}>
          {title}
          {pill}
        </Dialog.Title>
        {description ? (
          <Dialog.Description render={<div />}>{description}</Dialog.Description>
        ) : null}
      </div>
    </div>
  )
}

export function SourceIdentityHeader({
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
    <SourceHeader
      description={description ? <Markdown>{description}</Markdown> : null}
      leading={<ProviderLogo name={name} size="large" />}
      pill={originLabel ? <Pill color="graySubtle">{originLabel}</Pill> : null}
      title={
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
      }
    />
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
  className,
  hint,
  htmlFor,
  label,
}: {
  children: ReactNode
  className?: string
  hint?: ReactNode
  htmlFor?: string
  label?: ReactNode
}) {
  return (
    <div className={classNames(styles.fieldItem, className)}>
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
      {typeof hint === 'string' ? <Markdown>{hint}</Markdown> : hint}
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
