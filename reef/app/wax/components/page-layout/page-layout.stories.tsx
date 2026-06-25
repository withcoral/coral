import type { Meta, StoryObj } from '@storybook/react-vite'

import { createMemoryRouter, RouterProvider } from 'react-router'

import * as Button from '@/wax/components/button'
import { Typography } from '@/wax/components/typography'

import { CopyLinkButton, PageLayout, TopBar } from './page-layout'

/**
 * PageLayout provides a consistent layout structure for detail pages.
 * It includes a top bar with breadcrumbs, optional actions, and a copy link button.
 *
 * ## Usage
 *
 * Use PageLayout when:
 * - Building detail pages that need breadcrumb navigation
 * - You want consistent header styling with actions and copy link functionality
 *
 * ## Components
 *
 * - **PageLayout**: Full page layout with top bar and content area
 * - **TopBar**: Standalone header component with breadcrumbs and actions
 * - **CopyLinkButton**: Reusable button that copies the current URL to clipboard
 */
const meta = {
  component: PageLayout,
  decorators: [
    (Story) => {
      const router = createMemoryRouter([{ element: <Story />, path: '*' }], {
        initialEntries: ['/'],
      })
      return <RouterProvider router={router} />
    },
  ],
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Wax/PageLayout',
} satisfies Meta<typeof PageLayout>

export default meta
type Story = StoryObj<typeof meta>

const defaultBreadcrumbs = [
  { id: 'alerts', text: 'Alerts', to: '/alerts', type: 'link' as const },
  { id: 'detail', text: 'Critical Database Alert', type: 'text' as const },
]

const sampleContent = (
  <div style={{ padding: '24px' }}>
    <Typography.HeadingMedium>Page Content</Typography.HeadingMedium>
    <Typography.Body variant="secondary">
      This is the main content area of the page. It will scroll independently of the top bar.
    </Typography.Body>
  </div>
)

export const Default: Story = {
  args: {
    breadcrumbs: defaultBreadcrumbs,
    children: sampleContent,
  },
}

export const WithActions: Story = {
  args: {
    actions: (
      <Button.Container size="32" variant="secondary">
        <Button.Icon name="Settings" />
        <Button.Text>Settings</Button.Text>
      </Button.Container>
    ),
    breadcrumbs: defaultBreadcrumbs,
    children: sampleContent,
  },
}

export const WithMultipleActions: Story = {
  args: {
    actions: (
      <>
        <Button.Container size="32" variant="bare">
          <Button.Icon name="Download" />
        </Button.Container>
        <Button.Container size="32" variant="bare">
          <Button.Icon name="Share" />
        </Button.Container>
      </>
    ),
    breadcrumbs: defaultBreadcrumbs,
    children: sampleContent,
  },
}

export const WithoutCopyLink: Story = {
  args: {
    breadcrumbs: defaultBreadcrumbs,
    children: sampleContent,
    showCopyLink: false,
  },
}

export const DeepBreadcrumbs: Story = {
  args: {
    breadcrumbs: [
      { id: 'home', text: 'Home', to: '/', type: 'link' as const },
      { id: 'alerts', text: 'Alerts', to: '/alerts', type: 'link' as const },
      { id: 'category', text: 'Database', to: '/alerts/database', type: 'link' as const },
      { id: 'detail', text: 'Connection Pool Exhausted', type: 'text' as const },
    ],
    children: sampleContent,
  },
}

export const LongBreadcrumbText: Story = {
  args: {
    breadcrumbs: [
      { id: 'alerts', text: 'Alerts', to: '/alerts', type: 'link' as const },
      {
        id: 'detail',
        text: 'Critical Production Database Outage Affecting Multiple Services in US-East Region',
        type: 'text' as const,
      },
    ],
    children: sampleContent,
  },
}

// TopBar standalone story
export const TopBarOnly: StoryObj<typeof TopBar> = {
  render: () => (
    <TopBar
      actions={
        <Button.Container size="32" variant="secondary">
          <Button.Text>Action</Button.Text>
        </Button.Container>
      }
      breadcrumbs={defaultBreadcrumbs}
    />
  ),
}

// CopyLinkButton standalone story
export const CopyLinkButtonOnly: StoryObj<typeof CopyLinkButton> = {
  parameters: {
    layout: 'centered',
  },
  render: () => <CopyLinkButton />,
}
