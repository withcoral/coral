import { useId } from 'react'

import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import * as styles from './oauth-fields.css'

export interface OAuthField {
  defaultValue?: string
  key: string
  label: string
  secret: boolean
}

export function OAuthFields({
  disabled,
  fields,
  inputKey,
  onValueChange,
  values,
}: {
  disabled: boolean
  fields: OAuthField[]
  inputKey: string
  onValueChange: (key: string, value: string) => void
  values: Record<string, string>
}) {
  const idPrefix = useId()

  if (fields.length === 0) {
    return (
      <Typography.BodySmall variant="secondary">
        Click Add source to open your browser and complete sign-in.
      </Typography.BodySmall>
    )
  }

  return (
    <div className={styles.fields}>
      {fields.map(({ key, label, secret, defaultValue }) => {
        const inputName = `oauth:${inputKey}:${key}`
        const inputId = `${idPrefix}-${key}`

        return (
          <div key={key} className={styles.field}>
            <Typography.BodyStrong as="label" htmlFor={inputId} variant="primary">
              {label}
            </Typography.BodyStrong>
            <TextInput
              id={inputId}
              name={inputName}
              type={secret ? 'password' : 'text'}
              value={values[key] ?? ''}
              onChange={(value) => onValueChange(key, value)}
              placeholder={defaultValue || label}
              disabled={disabled}
            />
          </div>
        )
      })}
    </div>
  )
}
