import { Fragment, useId } from 'react'

import { Inputs, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'

import { FieldError } from './fields'
import * as styles from './providers.css'

/**
 * The scopes Coral requests, and the separator Coral sends them with. URL
 * scopes on one host that share a prefix show without it.
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
  const prefixes = sharedPrefixes([...value, ...suggestions])

  return (
    <div className={styles.scopesRow}>
      <div className={styles.scopesInput}>
        <Typography.BodySmallStrong as="label" htmlFor={id}>
          Requested scopes
        </Typography.BodySmallStrong>
        {prefixes.length > 0 && (
          <Typography.BodySmall as="p" id={`${id}-prefix`} variant="secondary">
            {prefixes.length === 1 ? 'Shared prefix:' : 'Shared prefixes:'}{' '}
            {prefixes.map((prefix, index) => (
              <Fragment key={prefix}>
                {index > 0 && ', '}
                <Typography.CodeSmallInline as="span" variant="secondary">
                  {prefix}
                </Typography.CodeSmallInline>
              </Fragment>
            ))}
          </Typography.BodySmall>
        )}
        <Inputs.TokenInput
          id={id}
          value={value}
          onChange={onChange}
          suggestions={suggestions}
          tokenLabel={(scope) => {
            const prefix = prefixes.find((p) => scope.startsWith(p) && scope !== p)
            return prefix ? `…/${scope.slice(prefix.length)}` : scope
          }}
          placeholder="Add a scope"
          invalid={Boolean(error)}
          ariaDescribedBy={
            [prefixes.length > 0 && `${id}-prefix`, error && `${id}-error`]
              .filter(Boolean)
              .join(' ') || undefined
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

/**
 * For each host with two or more URL scopes, the longest prefix, up to a
 * slash, that its scopes share. A scope may share a name with one on another
 * host; its tooltip tells them apart.
 */
function sharedPrefixes(scopes: string[]): string[] {
  const hosts = new Map<string, string[]>()
  for (const scope of new Set(scopes)) {
    const scheme = scope.indexOf('://')
    const slash = scheme < 0 ? -1 : scope.indexOf('/', scheme + 3)
    if (slash < 0) continue
    const host = scope.slice(0, slash + 1)
    hosts.set(host, [...(hosts.get(host) ?? []), scope])
  }
  return [...hosts.values()].flatMap((urls) => {
    if (urls.length < 2) return []
    let prefix = urls[0]
    for (const url of urls) {
      while (!url.startsWith(prefix)) prefix = prefix.slice(0, -1)
    }
    return [prefix.slice(0, prefix.lastIndexOf('/') + 1)]
  })
}
