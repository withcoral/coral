import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, within } from 'storybook/test'

import { workspace } from './providers.stories'
import { ProviderSettingsPage } from './settings-page'
import type { ProviderSettingsPageProps, ProviderSettingsView } from './types'

const provider: ProviderSettingsView = {
  id: '00000000-0000-4000-8000-000000000001',
  displayName: 'GitLab.com',
  namespace: 'gitlab',
  callRoot: 'coral.providers.gitlab',
  builtin: false,
  etag: 'provider-etag',
  path: '/workspaces/demo/providers/00000000-0000-4000-8000-000000000001',
  account: 'caller',
  enabled: true,
  toolsReady: true,
  connection: 'connected',
  canConnect: true,
  canReconnect: true,
  accountSettingIncompatible: false,
}
const member = { ...workspace, effectiveRole: 'member' }
const archived = { ...workspace, state: 'archived' }

const meta = {
  title: 'Providers/Settings',
  component: ProviderSettingsPage,
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: Story,
          action: () => ({ error: null, message: 'Provider name saved.' }),
        },
      ])
      return <Stub initialEntries={[`${provider.path}/settings`]} />
    },
  ],
  parameters: { layout: 'fullscreen', chromatic: { viewports: [390, 1280] } },
  args: {
    workspace,
    provider,
    source: {
      state: 'loaded',
      kind: 'openapi',
      location: {
        type: 'url',
        url: 'https://gitlab.com/api/v4/openapi.json?private_token=[redacted]',
        host: 'gitlab.com',
      },
    },
    form: { csrf: 'storybook-csrf', requestId: 'storybook-request' },
  } satisfies ProviderSettingsPageProps,
} satisfies Meta<typeof ProviderSettingsPage>
export default meta
type Story = StoryObj<typeof meta>

export const Owner: Story = {}

export const YourAccountNotConnected: Story = {
  args: { provider: { ...provider, connection: 'not_connected' } },
}

export const SharedAccount: Story = {
  args: { provider: { ...provider, account: 'fixed' } },
}

export const SharedAccountIncompatible: Story = {
  args: {
    provider: {
      ...provider,
      account: 'fixed',
      connection: 'not_connected',
      accountSettingIncompatible: true,
    },
  },
}

export const NoAccount: Story = {
  args: {
    provider: { ...provider, account: 'none' },
    source: {
      state: 'loaded',
      kind: 'mcp',
      location: { type: 'url', url: 'https://mcp.example.com/mcp', host: 'mcp.example.com' },
    },
  },
}

export const UploadedDocument: Story = {
  args: { source: { state: 'loaded', kind: 'openapi', location: { type: 'upload' } } },
}

export const SourceReadFailed: Story = {
  args: { source: { state: 'failed' } },
}

// Members read the account sections only: no source column is reserved.
export const Member: Story = {
  args: {
    workspace: member,
    source: undefined,
    provider: { ...provider, connection: 'not_connected' },
  },
}

export const MemberSharedAccount: Story = {
  args: {
    workspace: member,
    source: undefined,
    provider: { ...provider, account: 'fixed', connection: 'not_connected' },
  },
}

export const Archived: Story = {
  args: { workspace: archived },
}

export const LongName: Story = {
  args: {
    provider: {
      ...provider,
      displayName: 'GitLab self-managed instance for the platform engineering organization',
    },
  },
}

export const Renaming: Story = {
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await userEvent.click(canvas.getByRole('button', { name: 'Edit provider name' }))
    const input = canvas.getByRole('textbox', { name: 'Provider name' })
    await expect(input).toHaveFocus()
    await expect(input).toHaveValue('GitLab.com')
    await userEvent.clear(input)
    await userEvent.type(input, 'GitLab')
    await expect(canvas.getByRole('button', { name: 'Save' })).toBeEnabled()
    // Renaming changes the name only; the namespace stays fixed.
    await expect(canvas.queryByLabelText('Namespace')).not.toBeInTheDocument()
  },
}

export const RemovalConfirmation: Story = {
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await userEvent.click(canvas.getByRole('button', { name: 'Remove provider' }))
  },
}

export const Disabled: Story = {
  args: {
    provider: { ...provider, enabled: false },
  },
}

export const YourAccountReconnect: Story = {
  args: {
    provider: {
      ...provider,
      connection: 'needs_reconnect',
    },
  },
}

export const NoToolsReady: Story = {
  args: {
    provider: { ...provider, enabled: false, toolsReady: false },
  },
}

export const YourAccountUnreadable: Story = {
  args: { provider: { ...provider, connection: 'unknown' } },
}

// Coral manages the Built-in Provider: its settings offer no change.
export const BuiltinProvider: Story = {
  args: {
    source: undefined,
    provider: {
      ...provider,
      displayName: 'Coral',
      namespace: '',
      callRoot: 'coral.workspace',
      builtin: true,
      account: 'none',
    },
  },
}
