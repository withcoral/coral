import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { Typography } from '@/wax/components'

import { OnboardingPage } from './onboarding-page'
import { getOnboardingStepState } from './onboarding-steps'

const meta = {
  args: {
    action: {
      label: 'Continue',
      onClick: fn(),
    },
    children: (
      <div
        style={{ alignItems: 'center', display: 'flex', height: '100%', justifyContent: 'center' }}
      >
        <Typography.Body variant="tertiary">Onboarding panel</Typography.Body>
      </div>
    ),
    sideContent: (
      <>
        <Typography.BodyLarge>
          Use this shell for onboarding screens with explanatory copy on the left and the active
          setup surface on the right.
        </Typography.BodyLarge>
      </>
    ),
    sideTitle: 'Shared onboarding shell',
    step: getOnboardingStepState('sources'),
  },
  component: OnboardingPage,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/Onboarding/Page',
} satisfies Meta<typeof OnboardingPage>

export default meta
type Story = StoryObj<typeof meta>

export const Basic: Story = {}
