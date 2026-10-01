import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'

import { paletteDark } from '@/wax/base/palette/palette-dark'
import { Button } from '@/wax/components'
import { darkTheme } from '@/wax/theme/theme-dark.css'

import { Container } from './scroll-area'

const meta: Meta<typeof Container> = {
  args: {
    fade: 'both',
    horizontal: true,
  },
  argTypes: {
    constrainWidth: {
      control: 'boolean',
    },
    fade: {
      control: 'select',
      options: [undefined, 'top', 'bottom', 'both', 'horizontal', 'none'],
    },
    scrollDirection: {
      control: 'select',
      options: [undefined, 'both', 'horizontal', 'vertical'],
    },
  },
  component: Container,
  decorators: [
    (Story) => (
      <div className={darkTheme} style={{ backgroundColor: paletteDark.Gray['02'] }}>
        <Story />
      </div>
    ),
  ],
  parameters: {
    backgrounds: { default: 'dark' },
    layout: 'centered',
  },
  title: 'Wax/ScrollArea',
}

export default meta
type Story = StoryObj<typeof Container>

export const Default: Story = {
  render: ({ constrainWidth, fade, horizontal, scrollDirection }) => (
    <Container
      constrainWidth={constrainWidth}
      fade={fade}
      height="200px"
      horizontal={horizontal}
      scrollDirection={scrollDirection}
      width="300px"
    >
      <div
        style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(10, 100px)', padding: 20 }}
      >
        {Array.from({ length: 100 }, (_, i) => (
          <div
            key={i}
            style={{
              alignItems: 'center',
              backgroundColor: paletteDark.Gray['03'],
              borderRadius: 8,
              color: paletteDark.Gray['11'],
              display: 'flex',
              fontSize: 14,
              fontWeight: 500,
              height: 100,
              justifyContent: 'center',
            }}
          >
            {i + 1}
          </div>
        ))}
      </div>
    </Container>
  ),
}

/** Uses maxHeight - grows with content up to 300px, then scrolls */
export const MaxHeight: Story = {
  render: function MaxHeightStory({ constrainWidth, fade }) {
    const [count, setCount] = useState(2)

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <Button.TextButton onClick={() => setCount((c) => c + 1)}>
          Add item ({count} items)
        </Button.TextButton>
        <Container constrainWidth={constrainWidth} fade={fade} maxHeight="300px" width="300px">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: 20 }}>
            {Array.from({ length: count }, (_, i) => (
              <div
                key={i}
                style={{
                  alignItems: 'center',
                  backgroundColor: paletteDark.Gray['03'],
                  borderRadius: 8,
                  color: paletteDark.Gray['11'],
                  display: 'flex',
                  fontSize: 14,
                  fontWeight: 500,
                  height: 50,
                  justifyContent: 'center',
                }}
              >
                {i + 1}
              </div>
            ))}
          </div>
        </Container>
      </div>
    )
  },
}
