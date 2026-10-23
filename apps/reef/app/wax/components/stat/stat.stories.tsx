import type { Meta, StoryObj } from '@storybook/react-vite'
import * as Card from '../card'
import { Stat } from './stat'

const meta: Meta<typeof Stat> = {
  component: Stat,
  title: 'Wax/Stat',
  args: { label: 'Calls', icon: 'Activity', value: '458', detail: '90% answered' },
}
export default meta
type Story = StoryObj<typeof Stat>

export const Medium: Story = {
  decorators: [
    (Story) => (
      <Card.Container style={{ width: 220 }}>
        <Story />
      </Card.Container>
    ),
  ],
}
export const Small: Story = { args: { size: 'small', detail: undefined } }
// In a row of stats, the detail beside the value saves a line.
export const InlineDetail: Story = {
  args: { size: 'small', detail: '90%', detailPlacement: 'inline' },
}
// The value speaks its meaning when its sign alone is ambiguous.
export const Toned: Story = {
  args: {
    label: 'Context',
    icon: 'Layers',
    value: '−48.2 MB',
    valueLabel: '48.2 MB saved',
    detail: 'Saved',
    variant: 'success',
  },
}
