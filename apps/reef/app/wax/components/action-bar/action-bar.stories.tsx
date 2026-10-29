import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'

import { Button, Inputs, Typography } from '@/wax/components'

import { SCROLL_END_ROOM_PROPERTY } from '@/wax/components/table/constants'

import { Bar, Frame } from './action-bar'

const meta = {
  component: Frame,
  parameters: { layout: 'padded' },
  title: 'Wax/ActionBar',
} satisfies Meta<typeof Frame>
export default meta
type Story = StoryObj<typeof meta>

const ROWS = Array.from({ length: 30 }, (_, i) => `tool.${i + 1}`)

function Example({ initial = [] }: { initial?: string[] }) {
  const [selected, setSelected] = useState<string[]>(initial)
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 360 }}>
      <Frame>
        <div
          style={{
            flex: '1 1 auto',
            minHeight: 0,
            overflowY: 'auto',
            paddingBlockEnd: `var(${SCROLL_END_ROOM_PROPERTY}, 0px)`,
          }}
        >
          {ROWS.map((row) => (
            <div key={row} style={{ padding: '6px 12px' }}>
              <Inputs.Checkbox
                checked={selected.includes(row)}
                onCheckedChange={(checked) =>
                  setSelected((current) =>
                    checked ? [...current, row] : current.filter((r) => r !== row),
                  )
                }
              >
                {row}
              </Inputs.Checkbox>
            </div>
          ))}
        </div>
        {selected.length > 0 && (
          <Bar>
            <Typography.BodyStrong as="span">{`${selected.length} selected`}</Typography.BodyStrong>
            <Button.TextButton type="button">Apply</Button.TextButton>
            <Button.TextButton type="button" variant="bare" onClick={() => setSelected([])}>
              Clear selection
            </Button.TextButton>
          </Bar>
        )}
      </Frame>
    </div>
  )
}

export const Empty: Story = { args: { children: null }, render: () => <Example /> }

export const WithSelection: Story = {
  args: { children: null },
  render: () => <Example initial={['tool.2', 'tool.5']} />,
}
