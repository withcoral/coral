import type { Meta, StoryObj } from '@storybook/react-vite'

import { createMemoryRouter, RouterProvider } from 'react-router'

import { Breadcrumbs } from './breadcrumbs'

/**
 * Breadcrumbs provide a navigational aid that shows users their current location
 * within a site hierarchy. They display a path of links from the root to the current page.
 *
 * ## Usage
 *
 * Use breadcrumbs when:
 * - The site has a hierarchical structure with more than two levels
 * - Users need to understand their location within the site
 * - Users may want to navigate back to a parent page
 *
 * ## Segments
 *
 * Each breadcrumb item can be either:
 * - **link**: A clickable segment that navigates to another page (uses React Router's `Link`)
 * - **text**: A non-clickable segment, typically used for the current page
 */
const meta = {
  component: Breadcrumbs,
  decorators: [
    (Story) => {
      const router = createMemoryRouter([{ element: <Story />, path: '*' }], {
        initialEntries: ['/'],
      })
      return <RouterProvider router={router} />
    },
  ],
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Breadcrumbs',
} satisfies Meta<typeof Breadcrumbs>

export default meta
type Story = StoryObj<typeof meta>

const defaultItems = [
  { id: '1', text: 'Investigations', to: '/', type: 'link' as const },
  { id: '2', text: 'New', type: 'text' as const },
]

export const Default: Story = {
  args: {
    items: defaultItems,
  },
}

export const SingleLink: Story = {
  args: {
    items: [{ id: '1', text: 'Dashboard', type: 'text' as const }],
  },
}

export const TwoLevels: Story = {
  args: {
    items: [
      { id: '1', text: 'Alerts', to: '/alerts', type: 'link' as const },
      { id: '2', text: 'Alert Details', type: 'text' as const },
    ],
  },
}

export const DeepHierarchy: Story = {
  args: {
    items: [
      { id: '1', text: 'Home', to: '/', type: 'link' as const },
      { id: '2', text: 'Settings', to: '/settings', type: 'link' as const },
      { id: '3', text: 'Organization', to: '/settings/org', type: 'link' as const },
      { id: '4', text: 'Teams', to: '/settings/org/teams', type: 'link' as const },
      { id: '5', text: 'Engineering', type: 'text' as const },
    ],
  },
}

export const LongLabels: Story = {
  args: {
    items: [
      {
        id: '1',
        text: 'Investigation Dashboard Full of Issues and Solutions',
        to: '/',
        type: 'link' as const,
      },
      { id: '2', text: 'Active Incidents Overview', to: '/incidents', type: 'link' as const },
      {
        id: '3',
        text: 'Critical Production Database Outage - Region US-East',
        type: 'text' as const,
      },
    ],
  },
}
