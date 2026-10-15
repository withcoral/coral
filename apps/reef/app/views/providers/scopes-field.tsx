import { useId } from 'react'

import { Inputs, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'

import { FieldError } from './fields'
import * as styles from './providers.css'

/**
 * The scopes Coral requests, and the separator Coral sends them with. Scopes
 * that share a URL prefix show without it.
 */
export function ScopesField({
  value,
  onChange,
  suggestions,
  error,
  format,
  onFormatChange,
  formatError,
}: {
  value: string[]
  onChange: (scopes: string[]) => void
  suggestions: string[]
  error?: string
  format: 'space' | 'comma'
  onFormatChange: (format: 'space' | 'comma') => void
  formatError?: string
}) {
  const id = useId()
  const prefix = sharedPrefix([...value, ...suggestions])

  return (
    <div className={styles.scopesRow}>
      <div className={styles.scopesInput}>
        <Typography.BodySmallStrong as="label" htmlFor={id}>
          Requested scopes
        </Typography.BodySmallStrong>
        {prefix && (
          <Typography.BodySmall as="p" id={`${id}-prefix`} variant="secondary">
            Shared prefix:{' '}
            <Typography.CodeSmallInline as="span" variant="secondary">
              {prefix}
            </Typography.CodeSmallInline>
          </Typography.BodySmall>
        )}
        <Inputs.TokenInput
          id={id}
          value={value}
          onChange={onChange}
          suggestions={suggestions}
          tokenLabel={(scope) =>
            prefix && scope.startsWith(prefix) && scope !== prefix
              ? `…/${scope.slice(prefix.length)}`
              : scope
          }
          placeholder="Add a scope"
          invalid={Boolean(error)}
          ariaDescribedBy={
            [prefix && `${id}-prefix`, error && `${id}-error`].filter(Boolean).join(' ') ||
            undefined
          }
        />
        {error && <FieldError id={`${id}-error`} message={error} />}
      </div>
      <div className={styles.scopesFormat}>
        <Typography.BodySmallStrong as="span">Send scopes separated by</Typography.BodySmallStrong>
        <div className={styles.inlineControl}>
          <Picker
            ariaLabel="Send scopes separated by"
            value={format}
            options={[
              { value: 'space', label: 'Spaces' },
              { value: 'comma', label: 'Commas' },
            ]}
            onChange={(next) => onFormatChange(next as 'space' | 'comma')}
          />
        </div>
        <FieldError message={formatError} />
      </div>
    </div>
  )
}

/** The longest URL prefix, up to a slash, that two or more URL scopes share. */
function sharedPrefix(scopes: string[]): string | undefined {
  const urls = [...new Set(scopes)].filter((scope) => scope.includes('://'))
  if (urls.length < 2) return undefined
  let prefix = urls[0]
  for (const url of urls) {
    while (!url.startsWith(prefix)) prefix = prefix.slice(0, -1)
  }
  prefix = prefix.slice(0, prefix.lastIndexOf('/') + 1)
  return prefix.includes('://') && !prefix.endsWith('://') ? prefix : undefined
}
