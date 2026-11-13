import { useId, useState, type ReactNode } from 'react'
import { referenceInputName } from '@/lib/credential-field-names'
import { Inputs, Typography } from '@/wax/components'
import type { SavedReferenceView } from '@/views/providers/types'
import { Picker } from '@/wax/components/picker'
import { SegmentedControl } from '@/wax/components/segmented-control'
import * as styles from '@/views/management/credential-fields.css'
import type { OfferedSecretStore } from './types'
import { UNKNOWN_REFERENCE_FORM } from '@/lib/reference-form'

const SOURCES = [
  { value: 'value', label: 'Enter value' },
  { value: 'store', label: 'Use a secret store' },
] as const

/**
 * One Credential Field on a shared account: a typed value, or a reference to a secret
 * store entry. The chosen store says what the reference holds: a path and a key, or
 * one variable name. Only the chosen inputs are in the form.
 */
export function CredentialSource({
  value,
  field,
  label,
  stores,
  saved,
}: {
  /** The typed value's input, which the page's document form holds. */
  value: ReactNode
  field: string
  /** The field's label, so each field's controls have their own accessible names. */
  label: string
  stores: OfferedSecretStore[]
  /** The reference this field holds now, which opens filled in. */
  saved?: SavedReferenceView
}) {
  const [source, setSource] = useState<(typeof SOURCES)[number]['value']>(saved ? 'store' : 'value')
  const [store, setStore] = useState(saved?.store ?? stores[0]?.name)
  // A saved store stays on offer after it loses its grant, so saving says why, and
  // the reference never moves to another store on its own.
  const choices =
    saved && !stores.some(({ name }) => name === saved.store)
      ? [...stores, { name: saved.store, blockers: ['Unavailable'], form: UNKNOWN_REFERENCE_FORM }]
      : stores
  // Each store's kind says what its references hold, so the inputs follow the store.
  const form = choices.find(({ name }) => name === store)?.form ?? UNKNOWN_REFERENCE_FORM
  const kept = store === saved?.store ? saved : undefined
  const pathId = useId()
  const hintId = useId()
  const keyId = useId()
  return (
    <>
      <SegmentedControl
        label={`Value source for ${label}`}
        name={`source.${field}`}
        options={SOURCES}
        value={source}
        onChange={setSource}
      />
      {source === 'value' ? (
        value
      ) : (
        <div className={styles.list}>
          <div className={styles.field}>
            <Typography.BodySmallStrong as="span">Secret store</Typography.BodySmallStrong>
            <input type="hidden" name={referenceInputName(field, 'store')} value={store} />
            <Picker
              ariaLabel={`Secret store for ${label}`}
              options={choices.map(({ name, blockers }) => ({
                value: name,
                // A store Coral cannot read now says why in words, not by colour.
                label: blockers.length > 0 ? `${name} (${blockers.join(', ')})` : name,
              }))}
              value={store}
              onChange={setStore}
            />
          </div>
          {/* A new store starts empty, so a path for one kind never reaches another. */}
          <div className={styles.field} key={`path-${store}`}>
            <Typography.BodySmallStrong as="label" htmlFor={pathId}>
              {form.pathLabel}
            </Typography.BodySmallStrong>
            <Inputs.TextInput
              id={pathId}
              name={referenceInputName(field, 'path')}
              defaultValue={kept?.path}
              ariaDescribedBy={form.pathPrefix ? hintId : undefined}
              required
            />
            {form.pathPrefix && (
              <Typography.BodySmall id={hintId}>Starts with {form.pathPrefix}</Typography.BodySmall>
            )}
          </div>
          {form.key && (
            <div className={styles.field} key={`key-${store}`}>
              <Typography.BodySmallStrong as="label" htmlFor={keyId}>
                {form.key.label}
              </Typography.BodySmallStrong>
              <Inputs.TextInput
                id={keyId}
                name={referenceInputName(field, 'key')}
                defaultValue={kept?.key}
                required={form.key.required}
              />
            </div>
          )}
        </div>
      )}
    </>
  )
}
