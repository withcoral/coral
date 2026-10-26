import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'
import { Typography } from '@/wax/components/typography'
import { Divider } from './divider'

function SplitExample() {
  const [percent, setPercent] = useState(50)
  return (
    <div style={{ height: 400, display: 'flex', flexDirection: 'column' }}>
      <div style={{ flexGrow: percent, flexBasis: 0 }}>
        <Typography.Body>Detail pane</Typography.Body>
      </div>
      <Divider
        label="Resize example panes"
        percent={percent}
        onResize={(delta) =>
          setPercent((current) => Math.min(85, Math.max(15, current + delta / 4)))
        }
      />
      <div style={{ flexGrow: 100 - percent, flexBasis: 0 }}>
        <Typography.Body>Source pane</Typography.Body>
      </div>
    </div>
  )
}
const meta = {
  title: 'Wax/Divider',
  component: SplitExample,
  parameters: { layout: 'padded' },
} satisfies Meta<typeof SplitExample>
export default meta
type Story = StoryObj<typeof meta>
export const HorizontalSplit: Story = {
  play: async ({ canvasElement }) => {
    const separator = within(canvasElement).getByRole('separator', { name: 'Resize example panes' })
    await expect(separator).toHaveAttribute('aria-orientation', 'horizontal')
    await expect(separator).toHaveAttribute('aria-valuenow', '50')
  },
}
