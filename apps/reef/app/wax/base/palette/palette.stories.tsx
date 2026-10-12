import type { Meta, StoryObj } from '@storybook/react-vite'

import { paletteDark } from './palette-dark'
import { paletteLight } from './palette-light'
import type { PaletteValues, Scale } from './types'
import { theme } from '../../theme/theme.css'

const meta = {
  tags: ['autodocs'],
  title: 'Wax/Palette',
} satisfies Meta

export default meta
type Story = StoryObj<typeof meta>

const scales: Scale[] = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

const ColorSwatch = ({ color, label }: { color: string; label: string }) => (
  <div style={{ alignItems: 'center', display: 'flex', flexDirection: 'column', gap: 4 }}>
    <div
      style={{
        backgroundColor: color,
        border: '1px solid rgba(128, 128, 128, 0.3)',
        borderRadius: 4,
        height: 48,
        width: 96,
      }}
    />
    <span style={{ color: theme.content.tertiary, fontSize: 10 }}>{label}</span>
  </div>
)

const ColorRow = ({ colors, name }: { colors: Record<Scale, string>; name: string }) => (
  <div>
    <div style={{ color: theme.content.primary, fontSize: 12, fontWeight: 600, marginBottom: 8 }}>
      {name}
    </div>
    <div style={{ display: 'flex', gap: 4 }}>
      {scales.map((scale) => (
        <ColorSwatch color={colors[scale]} key={scale} label={scale} />
      ))}
    </div>
  </div>
)

const paletteNames: (keyof PaletteValues)[] = [
  'Gray',
  'GrayAlpha',
  'CoralGreen',
  'CoralGreenAlpha',
  'Green',
  'GreenAlpha',
  'Blue',
  'BlueAlpha',
  'Purple',
  'PurpleAlpha',
  'Amber',
  'AmberAlpha',
  'Orange',
  'OrangeAlpha',
  'Red',
  'RedAlpha',
  'BlackAlpha',
]

export const AllColors: Story = {
  render: (_args, context) => {
    const palette = context.globals.backgrounds.value === 'light' ? paletteLight : paletteDark

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 24, padding: 8 }}>
        {paletteNames.map((name) => (
          <ColorRow colors={palette[name]} key={name} name={name} />
        ))}
      </div>
    )
  },
}
