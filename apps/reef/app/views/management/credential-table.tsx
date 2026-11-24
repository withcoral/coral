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

/** The source a field starts on that is not a store: the value typed here. */
const VALUE = ''

/**
 * Each Credential Field as a row: the field, where its value comes from, and the
 * value or the reference. The Source menu offers a typed value or any store on
 * offer, so one choice picks both. The caller renders each typed value's input,
 * so it stays inside the caller's document form. Only the chosen inputs post.
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
        { label: 'Field', width: 'minmax(96px, 1fr)' },
        { label: 'Source', width: 'content' },
        { label: 'Value', width: 'minmax(160px, 3fr)' },
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
  const [source, setSource] = useState(saved?.store ?? VALUE)
  // A saved store stays on offer after it loses its grant, so saving says why, and
  // the reference never moves to another store on its own.
  const choices =
    saved && !stores.some(({ name }) => name === saved.store)
      ? [...stores, { name: saved.store, blockers: ['Unavailable'], form: UNKNOWN_REFERENCE_FORM }]
      : stores
  const store = choices.find(({ name }) => name === source)
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
          ariaLabel={`Source for ${field.label}`}
          fill
          value={source}
          // Two groups say where the value lives: saved with the account, or read
          // from a store on each use.
          options={[
            { value: VALUE, label: 'Typed value', group: 'Saved here' },
            ...choices.map(({ name, blockers }) => ({
              value: name,
              // A store Coral cannot read now says why in words, not by colour.
              label: blockers.length > 0 ? `${name} (${blockers.join(', ')})` : name,
              group: 'From secret store',
            })),
          ]}
          onChange={setSource}
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
          // A new store starts empty, so a path for one kind never reaches another.
          <span className={styles.referenceInputs} key={source}>
            <input type="hidden" name={referenceInputName(field.name, 'store')} value={source} />
            <span className={styles.referencePath}>
              <Inputs.TextInput
                bare
                id={`${id}-value`}
                name={referenceInputName(field.name, 'path')}
                ariaLabel={`${form.pathLabel} for ${field.label}`}
                ariaDescribedBy={form.pathPrefix ? `${id}-prefix` : undefined}
                // A label inside the box, so a filled one still says what it holds.
                prefix={form.pathLabel}
                placeholder={form.pathPrefix ? `${form.pathPrefix}…` : undefined}
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
                  prefix={form.key.label}
                  placeholder={form.key.required ? undefined : 'optional'}
                  defaultValue={kept?.key}
                  required={form.key.required}
                />
              </span>
            )}
          </span>
        )}
      </Table.Cell>
    </Table.Row>
  )
}
