import type { Meta, StoryObj } from '@storybook/react-vite'

import { Icon } from '@/wax/components/icon'

import { Pill } from './pill'

const meta: Meta<typeof Pill> = {
  argTypes: {
    color: {
      control: 'select',
      options: [
        'gray',
        'graySubtle',
        'green',
        'red',
        'blue',
        'amber',
        'orange',
        'purple',
        'mention',
      ],
    },
    size: {
      control: 'select',
      options: ['default', 'large'],
    },
  },
  component: Pill,
  title: 'Wax/Pill',
}

export default meta
type Story = StoryObj<typeof Pill>

export const Default: Story = {
  args: {
    children: 'New',
    color: 'gray',
  },
}

const AllColorsContent = () => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="gray">Gray</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Default / Neutral</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="graySubtle">Gray Subtle</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Subtle / Tertiary</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="green">Green</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Success / Active</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="red">Red</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Error / Critical</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="blue">Blue</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Info</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="amber">Amber</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Warning</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="orange">Orange</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Attention</span>
    </div>
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="purple">Purple</Pill>
      <span style={{ fontSize: '12px', opacity: 0.6 }}>Accent / Highlight</span>
    </div>
  </div>
)

export const AllColors: Story = {
  render: () => <AllColorsContent />,
}

export const WithDifferentLabels: Story = {
  render: () => (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
      <Pill color="green">New</Pill>
      <Pill color="blue">Updated</Pill>
      <Pill color="amber">Pending</Pill>
      <Pill color="red">Failed</Pill>
      <Pill color="gray">Draft</Pill>
    </div>
  ),
}

export const WithIcons: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
        <Pill color="green">
          <Icon color="success" name="CircleCheck" size="16" />
          Success
        </Pill>
        <Pill color="red">
          <Icon color="error" name="CircleAlert" size="16" />
          Error
        </Pill>
        <Pill color="amber">
          <Icon color="warning" name="Timer" size="16" />
          Pending
        </Pill>
        <Pill color="blue">
          <Icon color="info" name="Info" size="16" />
          Info
        </Pill>
        <Pill color="gray">
          <Icon color="secondary" name="Flag" size="16" />
          Featured
        </Pill>
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
        <Pill color="green">
          <Icon color="success" name="Sparkles" size="16" />
          New Feature
        </Pill>
        <Pill color="amber">
          <Icon color="warning" name="Zap" size="16" />
          Beta
        </Pill>
      </div>
    </div>
  ),
}

export const Large: Story = {
  args: {
    children: (
      <>
        <Icon color="info" name="Star" size="16" />
        Featured
      </>
    ),
    color: 'blue',
    size: 'large',
  },
}

export const Interactive: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <Pill as="button" color="gray" onClick={() => alert('Gray clicked')}>
          Gray
        </Pill>
        <Pill as="button" color="graySubtle" onClick={() => alert('Gray Subtle clicked')}>
          Gray Subtle
        </Pill>
        <Pill as="button" color="green" onClick={() => alert('Green clicked')}>
          Green
        </Pill>
        <Pill as="button" color="red" onClick={() => alert('Red clicked')}>
          Red
        </Pill>
        <Pill as="button" color="blue" onClick={() => alert('Blue clicked')}>
          Blue
        </Pill>
        <Pill as="button" color="amber" onClick={() => alert('Amber clicked')}>
          Amber
        </Pill>
        <Pill as="button" color="orange" onClick={() => alert('Orange clicked')}>
          Orange
        </Pill>
        <Pill as="button" color="purple" onClick={() => alert('Purple clicked')}>
          Purple
        </Pill>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <Pill as="button" color="green" onClick={() => alert('Success clicked')}>
          <Icon color="success" name="CircleCheck" size="16" />
          Success
        </Pill>
        <Pill as="button" color="blue" onClick={() => alert('Info clicked')}>
          <Icon color="info" name="Info" size="16" />
          Info
        </Pill>
        <Pill as="button" color="amber" onClick={() => alert('Pending clicked')}>
          <Icon color="warning" name="Timer" size="16" />
          Pending
        </Pill>
      </div>
    </div>
  ),
}

export const Active: Story = {
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
      <Pill color="gray" isActive>
        Gray
      </Pill>
      <Pill color="graySubtle" isActive>
        Gray Subtle
      </Pill>
      <Pill color="green" isActive>
        Green
      </Pill>
      <Pill color="red" isActive>
        Red
      </Pill>
      <Pill color="blue" isActive>
        Blue
      </Pill>
      <Pill color="amber" isActive>
        Amber
      </Pill>
      <Pill color="orange" isActive>
        Orange
      </Pill>
      <Pill color="purple" isActive>
        Purple
      </Pill>
      <Pill color="mention" isActive>
        Mention
      </Pill>
    </div>
  ),
}

export const WithTruncation: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div>
        <h4 style={{ fontSize: '12px', marginBottom: '8px' }}>
          Container width: 80px (text only, hover for full text)
        </h4>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ maxWidth: '80px' }}>
            <Pill color="blue" title="This is a very long label that will be truncated">
              This is a very long label that will be truncated
            </Pill>
          </div>
          <div style={{ maxWidth: '80px' }}>
            <Pill color="green" title="Another long label text">
              Another long label text
            </Pill>
          </div>
          <div style={{ maxWidth: '80px' }}>
            <Pill color="amber">Short</Pill>
          </div>
        </div>
      </div>
      <div>
        <h4 style={{ fontSize: '12px', marginBottom: '8px' }}>
          Container width: 120px (with icons, hover for full text)
        </h4>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ maxWidth: '120px' }}>
            <Pill color="green" title="Successfully completed the operation">
              <Icon color="success" name="CircleCheck" size="16" />
              Successfully completed the operation
            </Pill>
          </div>
          <div style={{ maxWidth: '120px' }}>
            <Pill color="red" title="Critical error occurred in the system">
              <Icon color="error" name="CircleAlert" size="16" />
              Critical error occurred in the system
            </Pill>
          </div>
          <div style={{ maxWidth: '120px' }}>
            <Pill color="amber" title="Pending review and approval">
              <Icon color="warning" name="Timer" size="16" />
              Pending review and approval
            </Pill>
          </div>
        </div>
      </div>
      <div>
        <h4 style={{ fontSize: '12px', marginBottom: '8px' }}>No container constraint (default)</h4>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
          <Pill color="blue">This label will display its full content without truncation</Pill>
        </div>
      </div>
    </div>
  ),
}
