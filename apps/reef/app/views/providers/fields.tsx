import { useId, useState, type ReactNode } from 'react'

import { Inputs, Radio, Typography } from '@/wax/components'

import type { AccountChoice } from './types'
import { accountHelp, accountLabels, useAccountAudience } from './account-labels'
import * as styles from './providers.css'

/** The domain's namespace grammar: letters, digits and underscores, starting with a letter. */
export const NAMESPACE = /^[A-Za-z][A-Za-z0-9_]*$/

export function FieldError({ message, id }: { message?: string; id?: string }) {
  return message ? (
    <Typography.BodySmall as="p" id={id} role="alert" variant="error">
      {message}
    </Typography.BodySmall>
  ) : null
}

/**
 * Marks a label whose field must be answered. The mark is visual only: a text
 * field or combobox carries `required` itself, but a choice menu's button
 * cannot, so its step names the answer when Next is pressed.
 */
export function RequiredMark() {
  return <span className={styles.requiredMark} />
}

/** One line of text a person types, with its label, its guidance and its error. */
export function TextField({
  label,
  aside,
  value,
  onChange,
  onBlur,
  error,
  description,
  maxLength = 2048,
  id: given,
  required = false,
}: {
  label: string
  /** The input's id, for a step that moves focus to it. */
  id?: string
  required?: boolean
  /** Guidance shown under the field. */
  description?: string
  /** Shown beside the label, such as where the value came from. */
  aside?: ReactNode
  value: string
  onChange: (value: string) => void
  onBlur?: () => void
  error?: string
  maxLength?: number
}) {
  const generated = useId()
  const id = given ?? generated
  return (
    <div className={styles.field}>
      <div className={styles.callPath}>
        <Typography.BodySmallStrong as="label" htmlFor={id}>
          {label}
          {required && <RequiredMark />}
        </Typography.BodySmallStrong>
        {aside}
      </div>
      <Inputs.TextInput
        id={id}
        required={required}
        value={value}
        onChange={onChange}
        onBlur={onBlur}
        maxLength={maxLength}
        invalid={Boolean(error)}
        ariaDescribedBy={error ? `${id}-error` : undefined}
      />
      {/* An error takes the guidance's place, right under the input. */}
      {error ? (
        <FieldError id={`${id}-error`} message={error} />
      ) : (
        description && (
          <Typography.BodySmall as="p" variant="tertiary">
            {description}
          </Typography.BodySmall>
        )
      )}
    </div>
  )
}

/** A namespace a person will usually accept for a display name or host label; empty when none fits. */
export function suggestedNamespace(from: string) {
  const cleaned = from
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
  return /^[a-z]/.test(cleaned) ? cleaned.slice(0, 64) : ''
}

/** The immutable namespace programs call the provider by, shown under `coral.providers.` as it is typed. */
export function NamespaceField({
  defaultValue = '',
  value: controlled,
  autoFocus,
  name = 'namespace',
  onChange,
  error,
  id: given,
}: {
  /** The input's id, for a step that moves focus to it. */
  id?: string
  /** The posted field; null keeps the value out of the form, for a page that carries it itself. */
  name?: string | null
  defaultValue?: string
  value?: string
  autoFocus?: boolean
  onChange?: (value: string) => void
  /** Why a well-formed namespace cannot be used, such as one another provider has. */
  error?: string
}) {
  const generated = useId()
  const id = given ?? generated
  const [local, setValue] = useState(defaultValue)
  const value = controlled ?? local
  const valid = value === '' || NAMESPACE.test(value)
  return (
    <div className={styles.field}>
      <Typography.BodySmallStrong as="label" htmlFor={id}>
        Namespace
        <RequiredMark />
      </Typography.BodySmallStrong>
      <div className={styles.prefixedField}>
        <Typography.Body as="span" variant="tertiary" className={styles.prefixText}>
          coral.providers.
        </Typography.Body>
        <div className={styles.grow}>
          <Inputs.TextInput
            id={id}
            autoFocus={autoFocus}
            name={name ?? undefined}
            placeholder="name"
            value={value}
            invalid={!valid || Boolean(error)}
            ariaDescribedBy={error ? `${id}-error` : undefined}
            required
            maxLength={64}
            onChange={(next) => {
              setValue(next)
              onChange?.(next)
            }}
          />
        </div>
      </div>
      {/* An error takes the guidance's place, right under the input. A malformed
          namespace turns the guidance itself into the error. */}
      {error ? (
        <FieldError id={`${id}-error`} message={error} />
      ) : (
        <Typography.BodySmall
          as="p"
          className={styles.longValue}
          variant={valid ? 'tertiary' : 'error'}
        >
          {valid
            ? 'Letters, digits and underscores, starting with a letter. You cannot change it later.'
            : 'Use letters, digits and underscores. Start with a letter.'}
        </Typography.BodySmall>
      )}
    </div>
  )
}

/** Whose account every call through this provider uses; posts as `account`. */
export function AccountField({
  choices,
  defaultValue,
  variant = 'plain',
  actions,
  contents,
  onChange,
}: {
  choices: readonly AccountChoice[]
  defaultValue: AccountChoice
  /** Cards carry each choice's consequence; plain radios explain only the current one. */
  variant?: 'plain' | 'card'
  /** A control on the right of a choice's card, shown while it is selected. */
  actions?: Partial<Record<AccountChoice, ReactNode>>
  /** Fields inside a choice's card below its copy, shown while it is selected. */
  contents?: Partial<Record<AccountChoice, ReactNode>>
  onChange?: (value: AccountChoice) => void
}) {
  const [value, setValue] = useState<AccountChoice>(defaultValue)
  const help = accountHelp(useAccountAudience())
  if (choices.length === 1)
    return (
      <div className={styles.field}>
        <input type="hidden" name="account" value={choices[0]} />
        <Typography.BodySmall as="p">
          {choices[0] === 'none' ? 'No account needed. ' : ''}
          {help[choices[0]]}
        </Typography.BodySmall>
      </div>
    )
  return (
    <div className={variant === 'card' ? styles.wideField : styles.field}>
      <Typography.BodySmallStrong as="span">Whose account does it use?</Typography.BodySmallStrong>
      <input type="hidden" name="account" value={value} />
      <Radio.Group
        value={value}
        layout={variant === 'card' ? 'stack' : 'row'}
        aria-label="Whose account does it use?"
        onValueChange={(next) => {
          setValue(next as AccountChoice)
          onChange?.(next as AccountChoice)
        }}
      >
        {choices.map((choice) => (
          <Radio.Item
            key={choice}
            value={choice}
            variant={variant}
            description={variant === 'card' ? help[choice] : undefined}
            action={value === choice ? actions?.[choice] : undefined}
            fields={value === choice ? contents?.[choice] : undefined}
          >
            {accountLabels[choice]}
          </Radio.Item>
        ))}
      </Radio.Group>
      {variant === 'plain' && (
        <Typography.BodySmall as="p" variant="tertiary">
          {help[value]}
        </Typography.BodySmall>
      )}
    </div>
  )
}
