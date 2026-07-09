import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { OAuthFields } from './oauth-fields'

const clientFields = [
  {
    defaultValue: 'coral-desktop',
    key: 'client_id',
    label: 'Client id',
    secret: false,
  },
  {
    key: 'client_secret',
    label: 'Client secret',
    secret: true,
  },
]

const meta = {
  args: {
    disabled: false,
    inputKey: 'GITHUB_TOKEN',
    onValueChange: fn(),
    values: {},
  },
  component: OAuthFields,
  decorators: [
    (Story) => (
      <div style={{ width: 520 }}>
        <Story />
      </div>
    ),
  ],
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Components/Sources/Install/OAuthFields',
} satisfies Meta<typeof OAuthFields>

export default meta
type Story = StoryObj<typeof meta>

export const ClientCredentials: Story = {
  args: {
    fields: clientFields,
  },
}

export const Disabled: Story = {
  args: {
    disabled: true,
    fields: clientFields,
    values: {
      client_id: 'local-coral-client',
      client_secret: 'example-secret',
    },
  },
}

export const NoAdditionalFields: Story = {
  args: {
    fields: [],
  },
}
