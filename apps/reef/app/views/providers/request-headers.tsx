import classNames from 'classnames'
import { Button, Inputs, Table, Typography } from '@/wax/components'
import { FieldError } from './fields'
import type { ConfigurationValues } from './types'
import * as styles from './providers.css'

const MAX_REQUEST_HEADERS = 64

export function RequestHeaders({
  value,
  onChange,
  errors = {},
  className,
}: {
  value: NonNullable<ConfigurationValues['requestHeaders']>
  onChange: (value: NonNullable<ConfigurationValues['requestHeaders']>) => void
  errors?: Record<string, string>
  className?: string
}) {
  const set = (index: number, patch: Partial<(typeof value)[number]>) =>
    onChange(value.map((item, i) => (i === index ? { ...item, ...patch } : item)))
  // Each problem names its header, so the table keeps one height per row.
  const problems = value.flatMap((header) => {
    const message = errors[`request_headers.${header.name}`]
    return message ? [`${header.name || 'New request header'}: ${message}`] : []
  })
  return (
    <div className={classNames(styles.fullRow, className)}>
      <Typography.BodySmallStrong as="h4">Request headers</Typography.BodySmallStrong>
      <Typography.BodySmall as="p">
        Send non-secret headers with each request to the provider.
      </Typography.BodySmall>
      <Table.Container
        ariaLabel="Request headers"
        variant="card"
        className={styles.credentialTable}
        columns={[
          { label: 'Name', width: 'minmax(160px, 1fr)' },
          { label: 'Value', width: 'minmax(160px, 2fr)' },
          { ariaLabel: 'Actions', width: 44 },
        ]}
      >
        <Table.Head />
        <Table.Body>
          {value.map((header, index) => (
            // Keyed by place, not name, so typing a name keeps the row and its focus.
            <Table.Row key={index}>
              <Table.Cell control>
                <Inputs.TextInput
                  bare
                  ariaLabel={`Request header ${index + 1}`}
                  value={header.name}
                  maxLength={256}
                  placeholder="x-api-version"
                  invalid={Boolean(errors[`request_headers.${header.name}`])}
                  onChange={(name) => set(index, { name })}
                />
              </Table.Cell>
              <Table.Cell control>
                <Inputs.TextInput
                  bare
                  ariaLabel={`Request header value ${index + 1}`}
                  value={header.value}
                  maxLength={4096}
                  placeholder="2022-11-28"
                  onChange={(next) => set(index, { value: next })}
                />
              </Table.Cell>
              <Table.Cell actions>
                <Button.IconButton
                  name="Trash"
                  variant="bare"
                  tooltipText={`Remove ${header.name || 'request header'}`}
                  onClick={() => onChange(value.filter((_, i) => i !== index))}
                />
              </Table.Cell>
            </Table.Row>
          ))}
          {value.length < MAX_REQUEST_HEADERS && (
            <Table.AddRow onClick={() => onChange([...value, { name: '', value: '' }])}>
              Add request header
            </Table.AddRow>
          )}
        </Table.Body>
      </Table.Container>
      <FieldError message={errors.request_headers} />
      {problems.map((message, index) => (
        <FieldError key={index} message={message} />
      ))}
    </div>
  )
}
