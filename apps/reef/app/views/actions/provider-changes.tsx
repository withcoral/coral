import { List, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Disclosure } from '@/wax/components/disclosure'
import * as DescriptionList from '@/wax/components/description-list'
import { Pill, type PillColor } from '@/wax/components/pill'
import type {
  ProviderChanges as Changes,
  ProviderEntryChange,
  ProviderFieldChange,
  ProviderSource,
} from '../activity/types'
import * as styles from './actions.css'

/** What an updateProvider call changes, beside its arguments. Each sent field replaces its saved
 * value, so the saved side shows what the call removes. Coral reads the saved values when the
 * review opens. */
export function ProviderChanges({ changes }: { changes: Changes }) {
  return (
    <Disclosure
      title={changes.kind === 'fields' ? `Changes (${changes.fields.length})` : 'Changes'}
      open
    >
      {changes.kind === 'missing' ? (
        <Typography.BodySmall role="status">
          The provider no longer exists. This call fails.
        </Typography.BodySmall>
      ) : (
        <>
          <Typography.BodySmall variant="tertiary">
            Each field replaces its saved value.
          </Typography.BodySmall>
          <DescriptionList.List>
            {changes.fields.map((change) => (
              <DescriptionList.Row key={fieldName(change)} label={fieldName(change)}>
                <FieldChange change={change} />
              </DescriptionList.Row>
            ))}
          </DescriptionList.List>
        </>
      )}
    </Disclosure>
  )
}

function fieldName(change: ProviderFieldChange) {
  return change.kind === 'source' ? 'source' : change.field
}

function FieldChange({ change }: { change: ProviderFieldChange }) {
  switch (change.kind) {
    case 'value':
      return (
        <DescriptionList.List>
          <DescriptionList.Row label="Saved">
            <Json value={change.saved} />
          </DescriptionList.Row>
          <DescriptionList.Row label="New">
            <Json value={change.requested} />
          </DescriptionList.Row>
        </DescriptionList.List>
      )
    case 'source':
      return (
        <DescriptionList.List>
          <DescriptionList.Row label="Saved">
            <Source source={change.saved} />
          </DescriptionList.Row>
          <DescriptionList.Row label="New">
            {change.requested ? (
              <Source source={change.requested} />
            ) : change.unreadable ? (
              'This call fails. An uploaded document has no URL to read again.'
            ) : (
              'The call reads the saved URL again.'
            )}
          </DescriptionList.Row>
        </DescriptionList.List>
      )
    case 'entries':
      return change.entries.length === 0 ? (
        'No entries'
      ) : (
        <List.Container as="ul">
          {change.entries.map((entry) => (
            <Entry key={entry.key} entry={entry} />
          ))}
        </List.Container>
      )
  }
}

/** Each status has its own word, so color is never the only signal. */
const statuses: Record<ProviderEntryChange['status'], { label: string; color: PillColor }> = {
  added: { label: 'Added', color: 'green' },
  removed: { label: 'Removed', color: 'red' },
  changed: { label: 'Changed', color: 'amber' },
  kept: { label: 'Kept', color: 'gray' },
  unknown: { label: 'Unknown', color: 'gray' },
}

function Entry({ entry }: { entry: ProviderEntryChange }) {
  const status = statuses[entry.status]
  return (
    <List.Item as="li">
      <span className={styles.entryKey}>
        <Typography.Code>{entry.key}</Typography.Code>
        <Pill color={status.color} size="small">
          {status.label}
        </Pill>
      </span>
      <DescriptionList.List>
        {entry.saved !== null && entry.status !== 'kept' && (
          <DescriptionList.Row label="Saved">
            <Json value={entry.saved} />
          </DescriptionList.Row>
        )}
        {entry.requested !== null && (
          <DescriptionList.Row label={entry.status === 'kept' ? 'Value' : 'New'}>
            <Json value={entry.requested} />
          </DescriptionList.Row>
        )}
      </DescriptionList.List>
    </List.Item>
  )
}

/** Coral writes compact JSON. An object or array reads as a block; anything else inline, so "2"
 * stays apart from 2. */
function Json({ value }: { value: string }) {
  return value.startsWith('{') || value.startsWith('[') ? (
    <CodeBlock value={value} format="json" />
  ) : (
    <Typography.Code>{value}</Typography.Code>
  )
}

function Source({ source }: { source: ProviderSource }) {
  switch (source.kind) {
    case 'openapi_document':
      return 'Uploaded document'
    case 'openapi_url':
      return (
        <>
          OpenAPI document at <Typography.Code>{source.url}</Typography.Code>
        </>
      )
    case 'mcp_url':
      return (
        <>
          MCP server at <Typography.Code>{source.url}</Typography.Code>
        </>
      )
    case 'unknown':
      return source.url ? <Typography.Code>{source.url}</Typography.Code> : 'Unknown source'
  }
}
