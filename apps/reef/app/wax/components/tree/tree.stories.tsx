import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'

import { Tree, type TreeNode } from './tree'

const nodes: TreeNode[] = [
  {
    id: 'github',
    label: 'github',
    detail: '9 of 10',
    href: '/providers/github',
    children: [
      {
        id: 'github/issues',
        label: 'issues',
        detail: '3',
        children: [
          {
            id: 'github/issues.list',
            label: 'issues.list',
            href: '/providers/github/tools/issues.list',
          },
          {
            id: 'github/issues.create',
            label: 'issues.create',
            href: '/providers/github/tools/issues.create',
          },
          {
            id: 'github/issues.close',
            label: 'issues.close',
            href: '/providers/github/tools/issues.close',
            dimmed: true,
            marker: 'hollow',
          },
        ],
      },
      {
        id: 'github/repos',
        label: 'repos',
        detail: '2',
        children: [
          { id: 'github/repos.get', label: 'repos.get', href: '/providers/github/tools/repos.get' },
          {
            id: 'github/repos.list',
            label: 'repos.list',
            href: '/providers/github/tools/repos.list',
          },
        ],
      },
    ],
  },
  {
    id: 'weather',
    label: 'weather',
    detail: '1 of 1',
    href: '/providers/weather',
    children: [
      {
        id: 'weather/getForecast',
        label: 'getForecast',
        href: '/providers/weather/tools/getForecast',
      },
    ],
  },
]

const meta = {
  component: Tree,
  title: 'Components/Tree',
  args: { label: 'Providers', nodes },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ width: 320 }}>
          <Stub initialEntries={['/providers']} />
        </div>
      )
    },
  ],
} satisfies Meta<typeof Tree>
export default meta
type Story = StoryObj<typeof meta>

export const Expanded: Story = {}
export const Collapsed: Story = { args: { defaultCollapsedIds: ['github', 'weather'] } }
export const Empty: Story = { args: { nodes: [], emptyMessage: 'No tools match' } }
