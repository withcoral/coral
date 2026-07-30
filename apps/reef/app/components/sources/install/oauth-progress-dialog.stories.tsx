import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { OAuthProgressDialog } from './oauth-progress-dialog'

const meta: Meta<typeof OAuthProgressDialog> = {
  args: {
    error: null,
    inputLabel: () => 'Github token',
    onCancel: fn(),
    progress: {
      authorizationUrl: 'https://github.com/login/device',
      inputKey: 'GITHUB_TOKEN',
      kind: 'awaiting-oauth',
      userCode: 'ABCD-EFGH',
      verificationUri: 'https://github.com/login/device',
      verificationUriComplete: 'https://github.com/login/device?user_code=ABCD-EFGH',
    },
  },
  component: OAuthProgressDialog,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/Sources/Install/OAuthProgressDialog',
}

export default meta
type Story = StoryObj<typeof OAuthProgressDialog>

export const DeviceCode: Story = {}

export const BrowserRedirect: Story = {
  args: {
    progress: {
      authorizationUrl: 'https://provider.example/oauth/authorize',
      inputKey: 'API_TOKEN',
      kind: 'awaiting-oauth',
      userCode: '',
      verificationUri: '',
      verificationUriComplete: '',
    },
  },
}

export const Starting: Story = {
  args: {
    progress: { kind: 'busy' },
  },
}

export const ExchangingToken: Story = {
  args: {
    progress: { inputKey: 'GITHUB_TOKEN', kind: 'oauth-callback-received' },
  },
}

export const Finishing: Story = {
  args: {
    progress: { inputKey: 'GITHUB_TOKEN', kind: 'oauth-completed' },
  },
}

export const Error: Story = {
  args: {
    error: 'GitHub denied the authorization request.',
    progress: { kind: 'idle' },
  },
}
