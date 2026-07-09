import type { Meta, StoryObj } from '@storybook/react-vite'

import { OAuthProgress } from './oauth-progress'

const meta = {
  args: {
    authorizationUrl: 'https://github.com/login/device',
    inputLabel: 'Github token',
    userCode: '',
    verificationUri: '',
    verificationUriComplete: '',
  },
  component: OAuthProgress,
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
  title: 'Components/Sources/Install/OAuthProgress',
} satisfies Meta<typeof OAuthProgress>

export default meta
type Story = StoryObj<typeof meta>

export const BrowserRedirect: Story = {}

export const DeviceCode: Story = {
  args: {
    userCode: 'ABCD-EFGH',
    verificationUri: 'https://github.com/login/device',
    verificationUriComplete: 'https://github.com/login/device?user_code=ABCD-EFGH',
  },
}
