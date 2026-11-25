import { useId, useState } from 'react'
import { credentialInputName, referenceInputName } from '@/lib/credential-field-names'
import { Inputs, Table, Typography } from '@/wax/components'
import type { CredentialFieldView, SavedReferenceView } from '@/views/providers/types'
import { Picker } from '@/wax/components/picker'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import type { OfferedSecretStore } from '@/views/accounts/types'
import type { CredentialFieldEntry } from './credential-fields'
import * as styles from './credential-fields.css'
import { UNKNOWN_REFERENCE_FORM } from '@/lib/reference-form'

/** A field's two types: a value typed here, or a reference to a secret store. */
const VALUE = 'value'
const STORE = 'store'

/**
 * Each Credential Field as a row: the field, its type, and the value or the
 * reference. The Type menu offers a typed value or a secret store, as Setup's
 * Headers table does; a reference then names its store, path and key. The caller
 * renders each typed value's input, so it stays inside the caller's document
 * form. Only the chosen inputs post.
 */
export function CredentialTable({
  fields,
  stores,
  saved,
  children,
}: {
  fields: CredentialFieldView[]
  stores: OfferedSecretStore[]
  /** The references the account holds now, by field name, which open filled in. */
  saved: Record<string, SavedReferenceView>
  children: CredentialFieldEntry
}) {
  return (
    <Table.Container
      ariaLabel="Credentials"
      variant="card"
      columns={[
        // A short name takes its own width, so the value keeps the rest.
        { label: 'Field', width: 'fit-content(200px)' },
        { label: 'Type', width: 'content' },
        { label: 'Value', width: 'minmax(160px, 1fr)' },
      ]}
    >
      <Table.Head />
      <Table.Body>
        {fields.map((field) => (
          <CredentialRow
            key={field.name}
            field={field}
            stores={stores}
            saved={saved[field.name]}
            value={children}
          />
        ))}
      </Table.Body>
    </Table.Container>
  )
}

function CredentialRow({
  field,
  stores,
  saved,
  value,
}: {
  field: CredentialFieldView
  stores: OfferedSecretStore[]
  saved?: SavedReferenceView
  value: CredentialFieldEntry
}) {
  const id = useId()
  // A saved store stays on offer after it loses its grant, so saving says why, and
  // the reference never moves to another store on its own.
  const choices =
    saved && !stores.some(({ name }) => name === saved.store)
      ? [...stores, { name: saved.store, blockers: ['Unavailable'], form: UNKNOWN_REFERENCE_FORM }]
      : stores
  const [type, setType] = useState(saved ? STORE : VALUE)
  const [source, setSource] = useState(
    saved?.store ?? (choices.find(({ blockers }) => blockers.length === 0) ?? choices[0])?.name,
  )
  const store = type === STORE ? choices.find(({ name }) => name === source) : undefined
  // Each store's kind says what its references hold, so the inputs follow the store.
  const form = store?.form ?? UNKNOWN_REFERENCE_FORM
  const kept = source === saved?.store ? saved : undefined
  return (
    <Table.Row>
      <Table.Cell>
        <span className={styles.fieldName}>
          <Typography.BodySmallStrong as="label" htmlFor={`${id}-value`}>
            {field.label}
          </Typography.BodySmallStrong>
          {field.description && (
            <Typography.BodySmall variant="tertiary">{field.description}</Typography.BodySmall>
          )}
        </span>
      </Table.Cell>
      <Table.Cell control>
        <Picker
          ariaLabel={`Type for ${field.label}`}
          fill
          value={type}
          options={[
            { value: VALUE, label: 'Value' },
            { value: STORE, label: 'Secret store' },
          ]}
          onChange={setType}
        />
      </Table.Cell>
      <Table.Cell control>
        {!store ? (
          value({
            id: `${id}-value`,
            name: credentialInputName(field.name),
            masked: field.sensitive,
            bare: true,
          })
        ) : (
          <span className={styles.referenceInputs}>
            <input
              type="hidden"
              name={referenceInputName(field.name, 'store')}
              value={store.name}
            />
            <span className={styles.referenceStore}>
              <Picker
                ariaLabel={`Secret store for ${field.label}`}
                fill
                value={store.name}
                options={choices.map(({ name, blockers }) => ({
                  value: name,
                  // A store Coral cannot read now says why in words, not by colour.
                  label: blockers.length > 0 ? `${name} (${blockers.join(', ')})` : name,
                }))}
                onChange={setSource}
              />
            </span>
            {/* A new store starts empty, so a path for one kind never reaches another. */}
            <span className={styles.referenceParts} key={store.name}>
              <span className={styles.referencePath}>
                <Inputs.TextInput
                  bare
                  id={`${id}-value`}
                  name={referenceInputName(field.name, 'path')}
                  ariaLabel={`${form.pathLabel} for ${field.label}`}
                  ariaDescribedBy={form.pathPrefix ? `${id}-prefix` : undefined}
                  placeholder={form.pathPrefix ? `${form.pathPrefix}…` : form.pathLabel}
                  defaultValue={kept?.path}
                  required
                />
                {form.pathPrefix && (
                  <span id={`${id}-prefix`} className={visuallyHidden}>
                    Starts with {form.pathPrefix}
                  </span>
                )}
              </span>
              {form.key && (
                <span className={styles.referenceKey}>
                  <Inputs.TextInput
                    bare
                    name={referenceInputName(field.name, 'key')}
                    ariaLabel={`${form.key.label} for ${field.label}`}
                    placeholder={
                      form.key.required ? form.key.label : `${form.key.label} (optional)`
                    }
                    defaultValue={kept?.key}
                    required={form.key.required}
                  />
                </span>
              )}
            </span>
          </span>
        )}
      </Table.Cell>
    </Table.Row>
  )
}
