import type { Meta, StoryObj } from '@storybook/react-vite'

import { SecretsInput } from './secrets-input'

const meta = {
  component: SecretsInput,
  decorators: [
    (Story) => (
      <div style={{ width: 500 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Wax/Inputs/SecretsInput',
} satisfies Meta<typeof SecretsInput>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    content: 'https://example.com/events/webhook/datadog/?key=abc123secretkey456',
    secrets: [[51, 72]],
  },
}

export const FullMask: Story = {
  args: {
    content: 'my-secret-api-key-12345',
  },
}

export const MultipleSecrets: Story = {
  args: {
    content: 'api_key=secret123&token=mytoken456',
    secrets: [
      [8, 17],
      [25, 35],
    ],
  },
}

export const LongContent: Story = {
  args: {
    content:
      'https://example.com/events/webhook/datadog/?key=verylongsecretkeythatgoesandgoesandgoesandgoesforaverylongtime',
  },
}
