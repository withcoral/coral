import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'

import { Banner, Button, Typography } from '@/wax/components'

import { PageLayout } from './page-layout'

const meta = {
  component: PageLayout,
  title: 'Wax/PageLayout',
  parameters: { layout: 'fullscreen', chromatic: { viewports: [390, 1440] } },
  decorators: [
    (Story) => {
      // Breadcrumb links need a router.
      const RoutesStub = createRoutesStub([
        {
          Component: () => (
            <div style={{ height: '100vh' }}>
              <Story />
            </div>
          ),
          path: '*',
        },
      ])
      return <RoutesStub initialEntries={['/']} />
    },
  ],
  args: {
    breadcrumbs: [{ id: 'providers', type: 'text', text: 'Providers' }],
    children: <Typography.Body as="p">Add a provider to start.</Typography.Body>,
  },
} satisfies Meta<typeof PageLayout>
export default meta
type Story = StoryObj<typeof meta>

const tools = Array.from({ length: 50 }, (_, i) => (
  <Typography.Body as="p" key={i}>
    Tool {i + 1}
  </Typography.Body>
))
// Split pages own their scrolling inside the bounded body.
const splitContent = <div style={{ height: '100%', overflowY: 'auto' }}>{tools}</div>

export const Standard: Story = {}
export const StandardOverflow: Story = { args: { children: tools } }
export const Wide: Story = { args: { variant: 'wide' } }
export const WideOverflow: Story = { args: { variant: 'wide', children: tools } }
export const Split: Story = { args: { variant: 'split', children: splitContent } }
export const SplitWithActions: Story = {
  args: {
    variant: 'split',
    breadcrumbs: [
      { id: 'providers', type: 'link', text: 'Providers', to: '/providers' },
      {
        id: 'provider',
        type: 'text',
        text: 'A provider with a long name that truncates on narrow screens',
      },
    ],
    actions: (
      <>
        <Button.TextButton variant="secondary">Refresh tools</Button.TextButton>
        <Button.TextButton>Add provider</Button.TextButton>
      </>
    ),
    header: <Banner variant="warning">This workspace is archived.</Banner>,
    children: splitContent,
  },
}
export const SplitWithActionsMobile: Story = {
  ...SplitWithActions,
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}

export const WideWithBreadcrumbs: Story = {
  args: {
    variant: 'wide',
    breadcrumbs: [
      { id: 'providers', type: 'link', text: 'Providers', to: '/providers' },
      { id: 'add', type: 'text', text: 'Add GitHub' },
    ],
  },
}
export const WideWithBreadcrumbsMobile: Story = {
  ...WideWithBreadcrumbs,
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}

export const SplitWithBreadcrumbs: Story = {
  args: {
    variant: 'split',
    breadcrumbs: [{ id: 'providers', type: 'text', text: 'Providers' }],
    children: splitContent,
  },
}

// The content carries the h1, so the current crumb is plain text.
export const TitledCompact: Story = {
  args: {
    titled: true,
    compact: true,
    children: (
      <>
        <Typography.HeadingLarge as="h1">Providers</Typography.HeadingLarge>
        <Typography.Body as="p">Add a provider to start.</Typography.Body>
      </>
    ),
  },
}
