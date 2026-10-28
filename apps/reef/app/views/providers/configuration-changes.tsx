import { Banner, Button, Table } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { countLabel } from '@/utils/count-label'
import { authenticationLabels, serverLabel } from './configuration-details'
import type { SetupView } from './types'

/**
 * Where a new read of the source suggests other values than the saved ones. A
 * dialog puts each saved value beside the new one.
 */
export function ConfigurationChanges({ setup }: { setup: SetupView }) {
  const differences = setup.configuration?.differences ?? []
  if (!differences.length) return null
  return (
    <Dialog.Root>
      <Banner
        title={`The new source changes ${countLabel(differences.length, 'setting')}`}
        action={
          <Dialog.Trigger render={<Button.Container variant="primary" size="32" />}>
            <Button.Text>Review changes</Button.Text>
          </Dialog.Trigger>
        }
      />
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xxl">
          <Dialog.Header>
            <Dialog.Title>Review changes</Dialog.Title>
            <Dialog.Close />
          </Dialog.Header>
          <Dialog.Description>To use a new value, change its setting.</Dialog.Description>
          <Table.Container
            ariaLabel="Saved and new values"
            variant="card"
            columns={[
              { label: 'Setting', width: { fill: 1 } },
              { label: 'Saved', width: { fill: 2 } },
              { label: 'New source', width: { fill: 2 } },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {differences.map((difference) => (
                <Table.Row key={difference.field}>
                  <Table.Cell wrap>{differenceLabel(difference.field)}</Table.Cell>
                  <Table.Cell wrap>
                    {differenceValue(difference.field, difference.saved, 'No value')}
                  </Table.Cell>
                  <Table.Cell wrap>
                    {differenceValue(difference.field, difference.document, 'No default')}
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function differenceLabel(field: string) {
  const labels: Record<string, string> = {
    display_name: 'Name',
    'authentication.method': 'Authentication',
    'authentication.username': 'Username',
    'authentication.password': 'Password',
    'authentication.authorization_url': 'Authorization URL',
    'authentication.token_url': 'Token URL',
    'authentication.scopes': 'Requested permissions',
    'authentication.scope_format': 'Scope separator',
  }
  if (labels[field]) return labels[field]
  if (field.startsWith('request_headers.'))
    return `Request header ${field.slice('request_headers.'.length)}`
  if (field.startsWith('authentication.headers.'))
    return `Header ${field.slice('authentication.headers.'.length)}`
  if (field.startsWith('authentication.parameters.'))
    return `Query parameter ${field.slice('authentication.parameters.'.length)}`
  const end = field.lastIndexOf('/servers')
  if (end < 0) return 'Configuration setting'
  const declaration = field.slice(0, end + '/servers'.length)
  const property = field.slice(declaration.length)
  const label = serverLabel(declaration)
  if (property.startsWith('.variables.'))
    return `${label} · ${property.slice('.variables.'.length)}`
  return label.startsWith('Server for ')
    ? `Server URL for ${label.slice('Server for '.length)}`
    : `${label} URL`
}

function differenceValue(field: string, value: string | undefined, fallback: string) {
  if (value === undefined) return fallback
  if (field === 'authentication.method') {
    const labels: Record<string, string> = {
      ...authenticationLabels,
      header_credentials: 'API keys in headers',
      basic_auth: 'Username and password',
      query_credentials: 'API keys in the query',
      oauth_authorization_code: 'OAuth authorization code',
      oauth_device_code: 'OAuth device code',
    }
    return labels[value] ?? value
  }
  return value
}
