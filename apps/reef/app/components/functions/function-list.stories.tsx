import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { FunctionList, type FunctionListItem } from './function-list'

const functions: FunctionListItem[] = [
  {
    description: 'Pull requests waiting for review in a repository.',
    name: 'review_queue',
    sources: ['github'],
  },
  {
    description: 'Incidents opened since a point in time, filtered by severity.',
    name: 'recent_incidents',
    sources: ['datadog', 'pagerduty'],
  },
  {
    description: 'Summarise customer conversations that need a response.',
    name: 'customer_follow_up',
    sources: ['intercom', 'linear', 'slack'],
  },
]

const meta = {
  args: {
    functions,
    onDelete: fn(),
  },
  component: FunctionList,
  parameters: {
    layout: 'padded',
  },
  render: (args) => (
    <div style={{ maxWidth: 1040 }}>
      <FunctionList {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/Functions/FunctionList',
} satisfies Meta<typeof FunctionList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const LongContent: Story = {
  args: {
    functions: [
      {
        description:
          'Correlates deployments, alerts, unresolved incidents, ownership information, and recent support escalations.',
        name: 'deployment_health_summary_for_critical_production_services',
        sources: [
          'amazon_cloudwatch_observability',
          'datadog',
          'incident_management_production',
          'pagerduty',
        ],
      },
    ],
  },
}
