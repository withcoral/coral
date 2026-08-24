import type { Meta, StoryObj } from '@storybook/react-vite'

import { isCustomIcon } from '@/wax/components/icon/custom-icons/custom-icons'
import { darkTheme } from '@/wax/theme/theme-dark.css'

import { Icon } from './icon'
import type { IconColor, IconName } from './icon'

const meta = {
  component: Icon,
  decorators: [
    (Story) => (
      <div className={darkTheme} style={{ padding: 24 }}>
        <Story />
      </div>
    ),
  ],
  parameters: {
    backgrounds: { default: 'dark' },
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Icon',
} satisfies Meta<typeof Icon>

export default meta
type Story = StoryObj<typeof meta>

const productIconNames = [
  'Activity',
  'ArrowDown',
  'ArrowUp',
  'Bot',
  'Calendar',
  'Check',
  'ChevronDown',
  'ChevronLeft',
  'ChevronRight',
  'CircleAlert',
  'CircleAlertFull',
  'CircleCheck',
  'CircleCheckFull',
  'Copy',
  'Coral',
  'ExternalLink',
  'FileCode',
  'Eye',
  'EyeOff',
  'Info',
  'Link',
  'Loader',
  'PanelLeft',
  'Plug',
  'RefreshCw',
  'Search',
  'Settings',
  'Terminal',
  'TriangleAlertFull',
  'X',
] as const satisfies readonly IconName[]

const sampleIconNames: IconName[] = [
  'Search',
  'Plus',
  'X',
  'ChevronDown',
  'ChevronRight',
  'Check',
  'Info',
  'Settings',
  'Bell',
  'Calendar',
]

const IconRow = ({ color, icons }: { color?: IconColor; icons: IconName[] }) => (
  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
    {icons.map((name) => (
      <Icon color={color} key={name} name={name} />
    ))}
  </div>
)

const ProductIconWithLabel = ({ name }: { name: IconName }) => (
  <div
    style={{
      alignItems: 'center',
      display: 'flex',
      flexDirection: 'column',
      gap: 6,
      width: 104,
    }}
  >
    <div style={{ alignItems: 'center', display: 'flex', gap: 8 }}>
      <Icon color="primary" name={name} />
      <Icon color="secondary" name={name} />
      <Icon color="tertiary" name={name} />
      <Icon color="disabled" name={name} />
    </div>
    <div style={{ color: '#94969C', fontSize: 10, textAlign: 'center', wordBreak: 'break-word' }}>
      {name}
    </div>
    <div style={{ color: '#6F737C', fontSize: 9 }}>{isCustomIcon(name) ? 'custom' : 'lucide'}</div>
  </div>
)

const Label = ({ children }: { children: React.ReactNode }) => (
  <div style={{ color: '#94969C', fontSize: 12, marginBlockEnd: 8 }}>{children}</div>
)

export const Default: Story = {
  args: { name: 'Search' },
  render: () => (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, maxWidth: 920 }}>
      {productIconNames.map((name) => (
        <ProductIconWithLabel key={name} name={name} />
      ))}
    </div>
  ),
}

export const Variants: Story = {
  args: { name: 'Search' },
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div>
        <Label>Primary (default)</Label>
        <IconRow icons={sampleIconNames} />
      </div>
      <div>
        <Label>Secondary</Label>
        <IconRow color="secondary" icons={sampleIconNames} />
      </div>
      <div>
        <Label>Tertiary</Label>
        <IconRow color="tertiary" icons={sampleIconNames} />
      </div>
      <div>
        <Label>Disabled</Label>
        <IconRow color="disabled" icons={sampleIconNames} />
      </div>
      <div>
        <Label>Size 16</Label>
        <div style={{ display: 'flex', gap: 12 }}>
          <Icon name="Search" size="16" />
          <Icon name="Plus" size="16" />
          <Icon name="Check" size="16" />
        </div>
      </div>
    </div>
  ),
}
