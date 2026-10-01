import type { Meta, StoryObj } from '@storybook/react-vite'

import { Info } from 'lucide-react'
import { useState } from 'react'

import { IconButton } from '@/wax/components/button/icon-button'
import { Icon } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill/pill'

import { Tooltip } from './tooltip'

const meta: Meta<typeof Tooltip> = {
  argTypes: {
    side: {
      control: 'select',
      options: ['top', 'bottom', 'left', 'right'],
    },
  },
  component: Tooltip,
  title: 'Wax/Tooltip',
}

export default meta
type Story = StoryObj<typeof Tooltip>

export const Default: Story = {
  args: {
    children: <button style={{ padding: '8px 16px' }}>Hover me</button>,
    content: 'This is a tooltip',
    side: 'top',
  },
}

export const WithIcon: Story = {
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <span>Metrics</span>
      <Tooltip content="View detailed metrics information">
        <button style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
          <Info size={16} />
        </button>
      </Tooltip>
    </div>
  ),
}

export const AllSides: Story = {
  render: () => (
    <div
      style={{
        alignItems: 'center',
        display: 'flex',
        gap: '48px',
        justifyContent: 'center',
        padding: '80px',
      }}
    >
      <Tooltip content="Top tooltip" side="top">
        <button style={{ padding: '8px 16px' }}>Top</button>
      </Tooltip>
      <Tooltip content="Bottom tooltip" side="bottom">
        <button style={{ padding: '8px 16px' }}>Bottom</button>
      </Tooltip>
      <Tooltip content="Left tooltip" side="left">
        <button style={{ padding: '8px 16px' }}>Left</button>
      </Tooltip>
      <Tooltip content="Right tooltip" side="right">
        <button style={{ padding: '8px 16px' }}>Right</button>
      </Tooltip>
    </div>
  ),
}

export const LongContent: Story = {
  args: {
    children: <button style={{ padding: '8px 16px' }}>Hover for more info</button>,
    content:
      'This is a longer tooltip that contains more information. It will wrap to multiple lines when the content exceeds the maximum width.',
    side: 'top',
  },
}

export const WithIconButton: Story = {
  render: () => (
    <div
      style={{
        alignItems: 'center',
        display: 'flex',
        gap: '16px',
        padding: '48px',
      }}
    >
      <IconButton name="Settings" tooltipText="Settings" variant="bare" />
      <IconButton name="Ellipsis" tooltipText="More options" variant="bare" />
      <IconButton name="X" tooltipText="Close" variant="bare" />
    </div>
  ),
}

export const WithPillButton: Story = {
  render: () => (
    <div
      style={{
        alignItems: 'center',
        display: 'flex',
        gap: '16px',
        padding: '48px',
      }}
    >
      <Tooltip content="Click to view all active alerts">
        <Pill as="button" color="red">
          <Icon color="error" name="TriangleAlert" size="16" />3 Alerts
        </Pill>
      </Tooltip>
      <Tooltip content="System is running normally">
        <Pill as="button" color="green">
          <Icon color="success" name="CircleCheck" size="16" />
          Healthy
        </Pill>
      </Tooltip>
      <Tooltip content="Click to filter by this tag">
        <Pill as="button" color="gray">
          production
        </Pill>
      </Tooltip>
    </div>
  ),
}

export const ShowOnlyWhenTruncated: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', padding: '48px' }}>
      <div>
        <p style={{ color: '#666', marginBottom: '8px' }}>Truncated text (tooltip shows):</p>
        <Tooltip content="This is the full text that was truncated" showOnlyWhenTruncated>
          <div
            style={{
              border: '1px solid #ccc',
              maxWidth: '150px',
              overflow: 'hidden',
              padding: '8px',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            This is a very long text that will be truncated
          </div>
        </Tooltip>
      </div>
      <div>
        <p style={{ color: '#666', marginBottom: '8px' }}>Non-truncated text (no tooltip):</p>
        <Tooltip content="You should not see this tooltip" showOnlyWhenTruncated>
          <div
            style={{
              border: '1px solid #ccc',
              maxWidth: '300px',
              overflow: 'hidden',
              padding: '8px',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            Short text
          </div>
        </Tooltip>
      </div>
    </div>
  ),
}

export const TruncationWithResize: Story = {
  render: function TruncationWithResizeStory() {
    const [width, setWidth] = useState(200)
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', padding: '48px' }}>
        <p style={{ color: '#666' }}>
          Drag the slider to resize the container. Tooltip appears only when text is truncated.
        </p>
        <input
          max={400}
          min={100}
          onChange={(e) => setWidth(Number(e.target.value))}
          style={{ width: '200px' }}
          type="range"
          value={width}
        />
        <span style={{ color: '#666' }}>Width: {width}px</span>
        <Tooltip
          content="This is the full text: A moderately long piece of text"
          showOnlyWhenTruncated
        >
          <div
            style={{
              border: '1px solid #ccc',
              maxWidth: `${width}px`,
              overflow: 'hidden',
              padding: '8px',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            A moderately long piece of text
          </div>
        </Tooltip>
      </div>
    )
  },
}

export const MultiLineTruncation: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', padding: '48px' }}>
      <div>
        <p style={{ color: '#666', marginBottom: '8px' }}>Multi-line truncated (tooltip shows):</p>
        <Tooltip
          content="Full text: Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
          showOnlyWhenTruncated
        >
          <div
            style={{
              border: '1px solid #ccc',
              display: '-webkit-box',
              maxWidth: '200px',
              overflow: 'hidden',
              padding: '8px',
              WebkitBoxOrient: 'vertical',
              WebkitLineClamp: 2,
            }}
          >
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor
            incididunt ut labore et dolore magna aliqua.
          </div>
        </Tooltip>
      </div>
      <div>
        <p style={{ color: '#666', marginBottom: '8px' }}>Multi-line not truncated (no tooltip):</p>
        <Tooltip content="You should not see this tooltip" showOnlyWhenTruncated>
          <div
            style={{
              border: '1px solid #ccc',
              display: '-webkit-box',
              maxWidth: '400px',
              overflow: 'hidden',
              padding: '8px',
              WebkitBoxOrient: 'vertical',
              WebkitLineClamp: 3,
            }}
          >
            Short text that fits.
          </div>
        </Tooltip>
      </div>
    </div>
  ),
}
