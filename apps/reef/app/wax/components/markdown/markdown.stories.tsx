import type { Meta, StoryObj } from '@storybook/react-vite'

import { Markdown } from './markdown'

const meta = {
  component: Markdown,
  tags: ['autodocs'],
  title: 'Wax/Markdown',
} satisfies Meta<typeof Markdown>

export default meta
type Story = StoryObj<typeof meta>

export const StructuredContent: Story = {
  args: {
    children:
      '# Provider setup\n\nUse `coral.providers.github` to discover tools.\n\n1. Add a provider.\n2. Connect a **provider account**.\n\n## Before you continue\n\n- Review the requested scopes.\n- Keep credentials private.\n\n```ts\nreturn await coral.providers.github.issues.list();\n```',
  },
}

export const UntrustedProviderContent: Story = {
  args: {
    children:
      'Provider prose with <script>alert("not executed")</script> and an image: ![Image description](https://example.com/tracking.png).\n\n[Unsafe link](javascript:alert(1))\n\nA long identifier: ' +
      'providerTool'.repeat(20),
  },
}

export const Default: Story = {
  args: {
    children:
      'Connect a **provider account** from your account settings. See [provider docs](https://example.com) for setup details.',
  },
}
