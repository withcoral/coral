import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { OnboardingNextStepsPage } from './onboarding-next-steps-page'
import { getOnboardingStepState } from './onboarding-steps'

const meta = {
  args: {
    mcpLaunchConfig: { status: 'unavailable' },
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

export const Desktop: Story = {
  args: {
    mcpLaunchConfig: {
      config: {
        args: ['mcp-stdio'],
        command: '/Applications/Coral.app/Contents/Resources/coral/coral',
      },
      status: 'success',
    },
    runtime: 'desktop',
  },
}
