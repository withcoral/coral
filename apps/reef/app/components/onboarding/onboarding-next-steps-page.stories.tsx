import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { OnboardingNextStepsPage } from './onboarding-next-steps-page'
import { getOnboardingStepState } from './onboarding-steps'

const meta = {
  args: {
    mcpClients: { clients: [], onWorkspaceChange: fn() },
    onContinue: fn(),
    runtime: 'web',
    step: getOnboardingStepState('next-steps'),
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
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

// Enough clients that the list scrolls under its sticky header.
export const Desktop: Story = {
  args: {
    mcpClients: {
      clients: ['Claude Code', 'Claude Desktop', 'Codex', 'Cursor', 'VS Code', 'Zed'].map(
        (name, index) => ({
          configuredWorkspace: index === 0 ? 'default' : undefined,
          id: name.toLowerCase().replaceAll(' ', '-'),
          name,
        }),
      ),
      onWorkspaceChange: fn(),
      pendingClientIds: ['codex'],
    },
    runtime: 'desktop',
  },
}
