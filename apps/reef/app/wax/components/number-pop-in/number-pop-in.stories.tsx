import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect } from 'storybook/test'
import { Button, Typography } from '@/wax/components'
import { NumberPopIn } from './number-pop-in'

const meta: Meta = { parameters: { layout: 'centered' }, title: 'Wax/NumberPopIn' }
export default meta
type Story = StoryObj

function Counter() {
  const [bytes, setBytes] = useState(1200)
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
      <Typography.CodeSmallInline variant="error" aria-label="Context impact">
        <NumberPopIn value={`+${(bytes / 1000).toFixed(1)} kB`} />
      </Typography.CodeSmallInline>
      <Button.TextButton variant="secondary" onClick={() => setBytes(bytes + 300)}>
        Add bytes
      </Button.TextButton>
    </div>
  )
}

export const Update: Story = {
  play: async ({ canvas, userEvent }) => {
    const value = canvas.getByLabelText('Context impact')
    // The first render shows the value at rest.
    await expect(value.getAnimations({ subtree: true })).toHaveLength(0)
    await userEvent.click(canvas.getByRole('button', { name: 'Add bytes' }))
    await expect(value.textContent).toBe('+1.5 kB')
    // Only the changed digit pops; the shared sign, digits and unit stay still.
    const popping = value.getAnimations({ subtree: true })
    await expect(popping).toHaveLength(1)
    await expect((popping[0].effect as KeyframeEffect).target?.textContent).toBe('5')
  },
  render: () => <Counter />,
}
