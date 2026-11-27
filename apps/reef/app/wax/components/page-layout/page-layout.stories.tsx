import type { Meta, StoryObj } from '@storybook/react-vite'
import type { ReactNode } from 'react'
import { createRoutesStub } from 'react-router'
import { expect, within } from 'storybook/test'

import { resizeViewport } from '@/wax/storybook/browser'

import { Banner, Button, Typography } from '@/wax/components'
import { Brand } from '@/wax/components/brand'

import { PageLayout, TopBar, type PageLayoutProps } from './page-layout'

// Without `panel`: story args flatten the props' union, which allows a panel only
// on a standard page that scrolls. The panel stories render it themselves.
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
} satisfies Meta<Omit<PageLayoutProps, 'panel'>>
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

// A page outside the navigation shows its own content in place of breadcrumbs.
// A long email truncates, and its title shows all of it.
const email = 'someone.with.a.long.name@a-long-company-domain.example.com'
export const TopBarWithStart: Story = {
  render: () => (
    <TopBar
      start={<Brand />}
      actions={
        <>
          <Typography.BodySmall as="span" variant="secondary" truncate title={email}>
            {email}
          </Typography.BodySmall>
          <Button.TextButton variant="secondary">Sign out</Button.TextButton>
        </>
      }
    />
  ),
}
export const TopBarWithStartMobile: Story = {
  ...TopBarWithStart,
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}

/** The surface at a point in the window, ignoring the transparent elements above it. */
function surfaceAt(x: number, y: number) {
  let element = document.elementFromPoint(x, y)
  while (element && getComputedStyle(element).backgroundColor === 'rgba(0, 0, 0, 0)')
    element = element.parentElement
  return element ? getComputedStyle(element).backgroundColor : null
}

const WHITE = 'rgb(255, 255, 255)'
const panel = (
  <Typography.Body as="p">
    A form that only comes in light colors, such as Stripe's.
  </Typography.Body>
)

function withPanel(content: ReactNode): Pick<Story, 'render'> {
  return {
    render: ({ breadcrumbs, children }) => (
      <PageLayout breadcrumbs={breadcrumbs} panel={content}>
        {children}
      </PageLayout>
    ),
  }
}

/**
 * A light panel beside the content, to the window's right and bottom edges. It
 * stays light in the dark theme.
 */
export const WithPanel: Story = {
  ...withPanel(panel),
  globals: { backgrounds: { value: 'dark' } },
  play: async ({ canvasElement }) => {
    if (!(await resizeViewport(1280, 800))) return
    const canvas = within(canvasElement)
    const text = canvas.getByText(/only comes in light colors/).getBoundingClientRect()
    await expect(text.left).toBeGreaterThan(
      canvas.getByText('Add a provider to start.').getBoundingClientRect().right,
    )
    await expect(surfaceAt(window.innerWidth - 20, window.innerHeight - 20)).toBe(WHITE)
    await expect(surfaceAt(20, window.innerHeight - 20)).not.toBe(WHITE)
  },
}

/** A narrow window puts the panel under the content, the window's full width. */
export const WithPanelNarrow: Story = {
  ...withPanel(panel),
  globals: { backgrounds: { value: 'dark' } },
  play: async ({ canvasElement }) => {
    if (!(await resizeViewport(600, 800))) return
    const canvas = within(canvasElement)
    await expect(
      canvas.getByText(/only comes in light colors/).getBoundingClientRect().top,
    ).toBeGreaterThan(canvas.getByText('Add a provider to start.').getBoundingClientRect().bottom)
    await expect(surfaceAt(20, window.innerHeight - 20)).toBe(WHITE)
    await expect(surfaceAt(window.innerWidth - 20, window.innerHeight - 20)).toBe(WHITE)
  },
}

/** A falsy panel, such as `condition && <Form />`, shows none. */
export const WithFalsePanel: Story = {
  ...withPanel(false),
  globals: { backgrounds: { value: 'dark' } },
  play: async () => {
    if (!(await resizeViewport(1280, 800))) return
    await expect(surfaceAt(window.innerWidth - 20, window.innerHeight - 20)).not.toBe(WHITE)
  },
}
