import type { Meta, StoryObj } from '@storybook/react-vite'

import { darkTheme } from './theme-dark.css'
import { lightTheme } from './theme-light.css'
import { theme } from './theme.css'

const meta = {
  tags: ['autodocs'],
  title: 'Wax/Elevation',
} satisfies Meta

export default meta
type Story = StoryObj<typeof meta>

const elevations = ['e1', 'e2', 'e3', 'e4'] as const

const ElevationSwatch = ({ background, level }: { background: string; level: string }) => (
  <div style={{ alignItems: 'center', display: 'flex', flexDirection: 'column', gap: 8 }}>
    <div
      style={{
        backgroundColor: background,
        borderRadius: 8,
        boxShadow: `${theme.elevation[level as keyof typeof theme.elevation]}`,
        height: 80,
        width: 120,
      }}
    />
    <span style={{ fontSize: 12 }}>{level}</span>
  </div>
)

const ElevationRow = ({ background, themeClass }: { background: string; themeClass: string }) => (
  <div className={themeClass} style={{ display: 'flex', gap: 32, padding: 32 }}>
    {elevations.map((level) => (
      <ElevationSwatch background={background} key={level} level={level} />
    ))}
  </div>
)

export const Light: Story = {
  render: () => <ElevationRow background="#FFFFFF" themeClass={lightTheme} />,
}

export const Dark: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: () => (
    <div style={{ color: '#fff' }}>
      <ElevationRow background="#272E3E" themeClass={darkTheme} />
    </div>
  ),
}
