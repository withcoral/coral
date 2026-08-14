import type { Meta, StoryObj } from '@storybook/react-vite'

import { darkTheme } from '@/wax/theme/theme-dark.css'
import { lightTheme } from '@/wax/theme/theme-light.css'
import { theme } from '@/wax/theme/theme.css'

import { CodeBlock } from './code-block'

const meta = {
  args: {
    code: `select
  pull.number,
  pull.title
from github.pulls(owner => $owner, repo => $repo) as pull
where pull.state = 'open'
order by pull.updated_at desc`,
    language: 'sql',
  },
  component: CodeBlock,
  tags: ['autodocs'],
  title: 'Components/CodeBlock',
} satisfies Meta<typeof CodeBlock>

export default meta
type Story = StoryObj<typeof meta>

export const Light: Story = {
  render: (args) => (
    <div className={lightTheme} style={{ backgroundColor: theme.surface.mainContent, padding: 24 }}>
      <CodeBlock {...args} />
    </div>
  ),
}

export const Dark: Story = {
  parameters: { backgrounds: { default: 'dark' } },
  render: (args) => (
    <div className={darkTheme} style={{ backgroundColor: theme.surface.mainContent, padding: 24 }}>
      <CodeBlock {...args} />
    </div>
  ),
}
