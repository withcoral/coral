import type { Meta, StoryObj } from '@storybook/react-vite'

import { Pill } from '@/wax/components/pill'

import { TruncatedList } from './truncated-list'

const labels = ['github', 'linear', 'slack', 'datadog', 'pagerduty']

function TruncatedListExample() {
  return (
    <div style={{ width: 220 }}>
      <TruncatedList
        getKey={(label) => label}
        items={labels}
        renderItem={(label) => <Pill color="gray">{label}</Pill>}
        renderOverflowContent={(hiddenLabels) =>
          hiddenLabels.map((label) => (
            <Pill color="gray" key={label}>
              {label}
            </Pill>
          ))
        }
        renderOverflowTrigger={(count) => (
          <Pill as="button" color="gray">
            +{count}
          </Pill>
        )}
      />
    </div>
  )
}

const meta = {
  component: TruncatedListExample,
  parameters: {
    layout: 'centered',
  },
  title: 'Components/TruncatedList',
} satisfies Meta<typeof TruncatedList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
