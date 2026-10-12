import { useId, type ReactNode } from 'react'
import { Typography } from '@/wax/components'
import { credentialInputName } from '@/lib/credential-field-names'
import type { CredentialFieldView } from '@/views/providers/types'
import * as styles from './credential-fields.css'

/** What one Credential Field's input needs: its id, form name and masking. */
export interface CredentialFieldInput {
  id: string
  name: string
  masked: boolean
}

/**
 * A label and help text for each Credential Field Coral asks for. The caller renders
 * each Wax CredentialInput inside its own document form, so values never enter
 * router submission state.
 */
export function CredentialFields({
  fields,
  children,
}: {
  fields: CredentialFieldView[]
  children: (input: CredentialFieldInput) => ReactNode
}) {
  const id = useId()
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
