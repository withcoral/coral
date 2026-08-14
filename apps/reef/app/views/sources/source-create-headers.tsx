import { useId } from 'react'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Text as ButtonText } from '@/wax/components/button/text'
import { TextInput } from '@/wax/components/inputs/text'

import * as styles from './source-create.css'
import { SourceField } from './source-presentation'

/** One header a request carries. An API needing several gets a row for each. */
export interface DraftHeader {
  id: number
  name: string
  value: string
}

export function HeaderCredentialsEditor({
  disabled,
  headers,
  onChange,
}: {
  disabled: boolean
  headers: DraftHeader[]
  onChange: (headers: DraftHeader[]) => void
}) {
  const idHeaderName = useId()
  const idHeaderToken = useId()
  const updateHeader = (index: number, patch: Partial<DraftHeader>) =>
    onChange(headers.map((header, at) => (at === index ? { ...header, ...patch } : header)))

  return (
    <>
      {headers.map((header, index) => (
        <div className={styles.headerRow} key={header.id}>
          <SourceField
            className={styles.fieldItem}
            htmlFor={`${idHeaderName}-${index}`}
            label="Header name"
          >
            <TextInput
              disabled={disabled}
              id={`${idHeaderName}-${index}`}
              onChange={(name) => updateHeader(index, { name })}
              placeholder="X-Api-Key"
              value={header.name}
            />
          </SourceField>
          <SourceField
            className={styles.fieldItem}
            htmlFor={`${idHeaderToken}-${index}`}
            label={`${header.name.trim() || 'Header'} value`}
          >
            <TextInput
              disabled={disabled}
              id={`${idHeaderToken}-${index}`}
              onChange={(value) => updateHeader(index, { value })}
              placeholder="Paste token"
              type="password"
              value={header.value}
            />
          </SourceField>
          {headers.length > 1 ? (
            <ButtonContainer
              ariaLabel={`Remove ${header.name.trim() || `header ${index + 1}`}`}
              className={styles.headerRemove}
              disabled={disabled}
              onClick={() => onChange(headers.filter(({ id }) => id !== header.id))}
              size="32"
              variant="bare"
            >
              <ButtonText>Remove header</ButtonText>
            </ButtonContainer>
          ) : null}
        </div>
      ))}
      <div className={styles.headerActions}>
        <ButtonContainer
          disabled={disabled}
          onClick={() =>
            onChange([
              ...headers,
              {
                id: Math.max(...headers.map(({ id }) => id)) + 1,
                name: '',
                value: '',
              },
            ])
          }
          size="32"
          variant="secondary"
        >
          <ButtonText>Add header</ButtonText>
        </ButtonContainer>
      </div>
    </>
  )
}

export function detectedHeaders(names: string[], current: DraftHeader[]): DraftHeader[] {
  return names.map((name, id) => ({
    id,
    name,
    value:
      current.find((header) => header.name.trim().toLowerCase() === name.trim().toLowerCase())
        ?.value ?? '',
  }))
}

/** Secret inputs backing custom headers, with collision-free normalized keys. */
export function headerInputs(
  headers: DraftHeader[],
): { key: string; name: string; value: string }[] {
  const taken = new Set<string>()
  return headers.map((header) => {
    const name = header.name.trim()
    const base = headerInputKey(name)
    let key = base
    let suffix = 2
    while (taken.has(key)) {
      key = `${base}_${suffix}`
      suffix += 1
    }
    taken.add(key)
    return { key, name, value: header.value.trim() }
  })
}

function headerInputKey(name: string): string {
  const key = name
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
  return /^[A-Z]/.test(key) ? key : `HEADER_${key}`
}
