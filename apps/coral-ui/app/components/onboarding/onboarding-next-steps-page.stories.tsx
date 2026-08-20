import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { OnboardingNextStepsPage } from './onboarding-next-steps-page'
import { getOnboardingStepState } from './onboarding-steps'

const meta = {
  args: {
    onContinue: fn(),
    runtime: 'web',
    step: getOnboardingStepState('next-steps'),
  },
  component: OnboardingNextStepsPage,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/Onboarding/NextStepsPage',
} satisfies Meta<typeof OnboardingNextStepsPage>

export default meta
type Story = StoryObj<typeof meta>

export const Web: Story = {}

// Desktop connects clients in place of the manual instructions. The list's own
// scrolling and pending states are covered by Components/McpClientsList.
export const Desktop: Story = {
  args: {
    mcpClients: {
      clients: [
        { configuredWorkspace: 'default', id: 'claude-code', name: 'Claude Code' },
        { id: 'codex', name: 'Codex' },
      ],
      onWorkspaceChange: fn(),
    },
    runtime: 'desktop',
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
  },
}
