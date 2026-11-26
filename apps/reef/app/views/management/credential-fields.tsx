import { useId, type ReactNode } from 'react'
import { Typography } from '@/wax/components'
import { credentialInputName } from '@/lib/credential-field-names'
import type { CredentialFieldView, SavedReferenceView } from '@/views/providers/types'
import type { OfferedSecretStore } from '@/views/accounts/types'
import { CredentialTable } from './credential-table'
import * as styles from './credential-fields.css'

/** What one Credential Field's input needs: its id, form name and masking. */
export interface CredentialFieldInput {
  id: string
  name: string
  masked: boolean
  /** Inside a table cell, which draws the edges. */
  bare?: boolean
}

/** Renders one field's typed value input, which the caller's document form holds. */
export type CredentialFieldEntry = (input: CredentialFieldInput) => ReactNode

/**
 * Whether the fields show as a table: when a store is on offer or a field holds a
 * reference. A dialog that holds them widens on the same rule.
 */
export function showsCredentialTable(
  stores: readonly unknown[] = [],
  saved: Record<string, unknown> = {},
) {
  return stores.length > 0 || Object.keys(saved).length > 0
}

/**
 * A label and help text for each Credential Field Coral asks for. The caller renders
 * each Wax CredentialInput inside its own document form, so values never enter
 * router submission state.
 */
export function CredentialFields({
  fields,
  children,
  stores = [],
  saved = {},
}: {
  fields: CredentialFieldView[]
  children: CredentialFieldEntry
  /** The stores a field can name instead of a typed value. Only a shared account has any. */
  stores?: OfferedSecretStore[]
  /** The references the account holds now, by field name, which open filled in. */
  saved?: Record<string, SavedReferenceView>
}) {
  const id = useId()
  // With a store to choose, each field also says where its value comes from.
  if (showsCredentialTable(stores, saved))
    return (
      <CredentialTable fields={fields} stores={stores} saved={saved}>
        {children}
      </CredentialTable>
    )
  return (
    <div className={styles.list}>
      {fields.map((field) => (
        <div key={field.name} className={styles.field}>
          <Typography.BodySmallStrong as="label" htmlFor={`${id}-${field.name}`}>
            {field.label}
          </Typography.BodySmallStrong>
          {children({
            id: `${id}-${field.name}`,
            name: credentialInputName(field.name),
            masked: field.sensitive,
          })}
          {field.description && (
            <Typography.BodySmall as="p" variant="tertiary">
              {field.description}
            </Typography.BodySmall>
          )}
        </div>
      ))}
    </div>
  )
}
