import type { Meta, StoryObj } from '@storybook/react-vite'

import { paletteDark } from './palette-dark'
import { paletteLight } from './palette-light'
import type { Scale } from './types'

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
    <span style={{ color: '#888', fontSize: 10 }}>{label}</span>
  </div>
)

const ColorRow = ({ colors, name }: { colors: Record<Scale, string>; name: string }) => (
  <div>
    <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8 }}>{name}</div>
    <div style={{ display: 'flex', gap: 4 }}>
      {scales.map((scale) => (
        <ColorSwatch color={colors[scale]} key={scale} label={scale} />
      ))}
    </div>
  </div>
)

const ColorPair = ({
  alpha,
  alphaName,
  solid,
  solidName,
}: {
  alpha: Record<Scale, string>
  alphaName: string
  solid: Record<Scale, string>
  solidName: string
}) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
    <ColorRow colors={solid} name={solidName} />
    <ColorRow colors={alpha} name={alphaName} />
  </div>
)

// Light mode stories
export const LightGray: Story = {
  render: () => (
    <ColorPair
      alpha={paletteLight.GrayAlpha}
      alphaName="GrayAlpha"
      solid={paletteLight.Gray}
      solidName="Gray"
    />
  ),
}

export const LightCoralGreen: Story = {
  render: () => (
    <ColorPair
      alpha={paletteLight.CoralGreenAlpha}
      alphaName="CoralGreenAlpha"
      solid={paletteLight.CoralGreen}
      solidName="CoralGreen"
    />
  ),
}

export const LightGreen: Story = {
  render: () => (
    <ColorPair
      alpha={paletteLight.GreenAlpha}
      alphaName="GreenAlpha"
      solid={paletteLight.Green}
      solidName="Green"
    />
  ),
}

export const LightBlue: Story = {
  render: () => (
    <ColorPair
      alpha={paletteLight.BlueAlpha}
      alphaName="BlueAlpha"
      solid={paletteLight.Blue}
      solidName="Blue"
    />
  ),
}

export const LightAmber: Story = {
  render: () => (
    <ColorPair
      alpha={paletteLight.AmberAlpha}
      alphaName="AmberAlpha"
      solid={paletteLight.Amber}
      solidName="Amber"
    />
  ),
}

export const LightRed: Story = {
  render: () => (
    <ColorPair
      alpha={paletteLight.RedAlpha}
      alphaName="RedAlpha"
      solid={paletteLight.Red}
      solidName="Red"
    />
  ),
}

export const LightBlackAlpha: Story = {
  render: () => <ColorRow colors={paletteLight.BlackAlpha} name="BlackAlpha" />,
}

// Dark mode stories
export const DarkGray: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorPair
        alpha={paletteDark.GrayAlpha}
        alphaName="GrayAlpha"
        solid={paletteDark.Gray}
        solidName="Gray"
      />
    </div>
  ),
}

export const DarkCoralGreen: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorPair
        alpha={paletteDark.CoralGreenAlpha}
        alphaName="CoralGreenAlpha"
        solid={paletteDark.CoralGreen}
        solidName="CoralGreen"
      />
    </div>
  ),
}

export const DarkGreen: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorPair
        alpha={paletteDark.GreenAlpha}
        alphaName="GreenAlpha"
        solid={paletteDark.Green}
        solidName="Green"
      />
    </div>
  ),
}

export const DarkBlue: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorPair
        alpha={paletteDark.BlueAlpha}
        alphaName="BlueAlpha"
        solid={paletteDark.Blue}
        solidName="Blue"
      />
    </div>
  ),
}

export const DarkAmber: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorPair
        alpha={paletteDark.AmberAlpha}
        alphaName="AmberAlpha"
        solid={paletteDark.Amber}
        solidName="Amber"
      />
    </div>
  ),
}

export const DarkRed: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorPair
        alpha={paletteDark.RedAlpha}
        alphaName="RedAlpha"
        solid={paletteDark.Red}
        solidName="Red"
      />
    </div>
  ),
}

export const DarkBlackAlpha: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ColorRow colors={paletteDark.BlackAlpha} name="BlackAlpha" />
    </div>
  ),
}
