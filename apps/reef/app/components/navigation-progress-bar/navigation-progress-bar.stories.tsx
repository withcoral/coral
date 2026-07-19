import type { Meta, StoryObj } from '@storybook/react-vite'

import { useEffect } from 'react'
import { createRoutesStub, useNavigate } from 'react-router'

import { Typography } from '@/wax/components/typography'
import { theme } from '@/wax/theme/theme.css'

import { NavigationProgressBar } from './navigation-progress-bar'

const PENDING_ROUTE = '/loading'

function createPendingLoader() {
  return new Promise(() => undefined)
}

function TriggerPendingNavigation() {
  const navigate = useNavigate()

  useEffect(() => {
    const timeout = window.setTimeout(() => void navigate(PENDING_ROUTE), 200)
    return () => window.clearTimeout(timeout)
  }, [navigate])

  return null
}

function DemoRoute() {
  return (
    <div
      style={{
        backgroundColor: theme.surface.main,
        minBlockSize: 320,
        overflow: 'hidden',
        padding: 24,
        position: 'relative',
        // Create a containing block so the fixed production bar stays inside each
        // Storybook theme-comparison panel instead of spanning the preview iframe.
        transform: 'translateZ(0)',
      }}
    >
      <NavigationProgressBar />
      <TriggerPendingNavigation />
      <section style={{ maxInlineSize: 480 }}>
        <Typography.HeadingMedium>Navigation in progress</Typography.HeadingMedium>
        <Typography.Body as="p" variant="secondary">
          The story starts a pending React Router navigation so the global progress bar appears
          after its short delay.
        </Typography.Body>
      </section>
    </div>
  )
}

const NavigationProgressBarRoutesStub = createRoutesStub([
  {
    Component: DemoRoute,
    path: '/',
  },
  {
    Component: DemoRoute,
    loader: createPendingLoader,
    path: PENDING_ROUTE,
  },
])

function NavigationProgressBarStory() {
  return <NavigationProgressBarRoutesStub initialEntries={['/']} />
}

const meta = {
  component: NavigationProgressBar,
  parameters: {
    layout: 'fullscreen',
  },
  render: () => <NavigationProgressBarStory />,
  tags: ['autodocs'],
  title: 'Components/NavigationProgressBar',
} satisfies Meta<typeof NavigationProgressBar>

export default meta
type Story = StoryObj<typeof meta>

export const Active: Story = {}
