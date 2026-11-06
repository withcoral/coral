import { useId, useState, type ReactNode } from 'react'
import { referenceInputName } from '@/lib/credential-field-names'
import { Inputs, Typography } from '@/wax/components'
import type { SavedReferenceView } from '@/views/providers/types'
import { Picker } from '@/wax/components/picker'
import { SegmentedControl } from '@/wax/components/segmented-control'
import * as styles from '@/views/management/credential-fields.css'

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
  stores,
  saved,
}: {
  /** The typed value's input, which the page's document form holds. */
  value: ReactNode
  field: string
  stores: string[]
  /** The reference this field holds now, which opens filled in. */
  saved?: SavedReferenceView
}) {
  const [source, setSource] = useState<(typeof SOURCES)[number]['value']>(saved ? 'store' : 'value')
  const [store, setStore] = useState(saved?.store ?? stores[0])
  // A saved store stays on offer after it loses its grant, so saving says why, and
  // the reference never moves to another store on its own.
  const choices = saved && !stores.includes(saved.store) ? [...stores, saved.store] : stores
  const pathId = useId()
  const keyId = useId()
  return (
    <>
      <SegmentedControl
        label="Value source"
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
              ariaLabel="Secret store"
              options={choices.map((name) => ({ value: name, label: name }))}
              value={store}
              onChange={setStore}
            />
          </div>
          <div className={styles.field}>
            <Typography.BodySmallStrong as="label" htmlFor={pathId}>
              Path
            </Typography.BodySmallStrong>
            <Inputs.TextInput
              id={pathId}
              name={referenceInputName(field, 'path')}
              defaultValue={saved?.path}
              required
            />
          </div>
          <div className={styles.field}>
            <Typography.BodySmallStrong as="label" htmlFor={keyId}>
              Key
            </Typography.BodySmallStrong>
            <Inputs.TextInput
              id={keyId}
              name={referenceInputName(field, 'key')}
              defaultValue={saved?.key}
              required
            />
          </div>
        </div>
      )}
    </>
  )
}
