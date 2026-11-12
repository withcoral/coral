import { useId, useState, type ReactNode } from 'react'
import { referenceInputName } from '@/lib/credential-field-names'
import { Inputs, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'
import { SegmentedControl } from '@/wax/components/segmented-control'
import * as styles from '@/views/management/credential-fields.css'
import type { OfferedSecretStore } from './types'

const SOURCES = [
  { value: 'value', label: 'Enter value' },
  { value: 'store', label: 'Use a secret store' },
] as const

/**
 * One Credential Field on a shared account: a typed value, or the store, path and key
 * of a secret store entry. Only the chosen inputs are in the form.
 */
export function CredentialSource({
  value,
  field,
  label,
  stores,
}: {
  /** The typed value's input, which the page's document form holds. */
  value: ReactNode
  field: string
  /** The field's label, so each field's controls have their own accessible names. */
  label: string
  stores: OfferedSecretStore[]
}) {
  const [source, setSource] = useState<(typeof SOURCES)[number]['value']>('value')
  const [store, setStore] = useState(stores[0]?.name)
  const pathId = useId()
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
              options={stores.map(({ name, blockers }) => ({
                value: name,
                // A store Coral cannot read now says why in words, not by colour.
                label: blockers.length > 0 ? `${name} (${blockers.join(', ')})` : name,
              }))}
              value={store}
              onChange={setStore}
            />
          </div>
          <div className={styles.field}>
            <Typography.BodySmallStrong as="label" htmlFor={pathId}>
              Path
            </Typography.BodySmallStrong>
            <Inputs.TextInput id={pathId} name={referenceInputName(field, 'path')} required />
          </div>
          <div className={styles.field}>
            <Typography.BodySmallStrong as="label" htmlFor={keyId}>
              Key
            </Typography.BodySmallStrong>
            <Inputs.TextInput id={keyId} name={referenceInputName(field, 'key')} required />
          </div>
        </div>
      )}
    </>
  )
}
