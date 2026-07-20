import type { Meta, StoryObj } from '@storybook/react-vite'

import { expect, fn } from 'storybook/test'

import { Tabs } from '@/wax/components'

const meta: Meta = {
  parameters: {
    layout: 'centered',
  },
  title: 'Wax/Tabs',
}

export default meta
interface DefaultArgs {
  onValueChange: (value: number | string) => void
}

export const Default: StoryObj<DefaultArgs> = {
  args: {
    onValueChange: fn(),
  },
  render: (args) => (
    <Tabs.Root defaultValue="summary" onValueChange={args.onValueChange}>
      <Tabs.List>
        <Tabs.Tab value="summary">Summary</Tabs.Tab>
        <Tabs.Tab value="incident-report">Incident report</Tabs.Tab>
        <Tabs.Tab value="cause-graph">Cause graph</Tabs.Tab>
        <Tabs.Tab disabled value="remediations">
          Remediations
        </Tabs.Tab>
        <Tabs.Indicator />
      </Tabs.List>
      <Tabs.Panel value="summary">Summary content</Tabs.Panel>
      <Tabs.Panel value="incident-report">Incident report content</Tabs.Panel>
      <Tabs.Panel value="cause-graph">Cause graph content</Tabs.Panel>
      <Tabs.Panel value="remediations">Remediations content</Tabs.Panel>
    </Tabs.Root>
  ),
}

const scrollTabs = [
  { label: 'Summary', value: 'summary' },
  { label: 'Incident report', value: 'incident-report' },
  { label: 'Cause graph', value: 'cause-graph' },
  { label: 'Remediations', value: 'remediations' },
  { label: 'Timeline', value: 'timeline' },
  { label: 'Related alerts', value: 'related-alerts' },
  { label: 'Runbooks', value: 'runbooks' },
  { label: 'Audit log', value: 'audit-log' },
  { label: 'Notifications', value: 'notifications' },
  { label: 'Configuration', value: 'configuration' },
]

export const HorizontalScroll: StoryObj<DefaultArgs> = {
  args: {
    onValueChange: fn(),
  },
  render: (args) => (
    <div style={{ maxWidth: 400 }}>
      <Tabs.Root defaultValue="summary" onValueChange={args.onValueChange}>
        <Tabs.List>
          {scrollTabs.map((tab) => (
            <Tabs.Tab key={tab.value} value={tab.value}>
              {tab.label}
            </Tabs.Tab>
          ))}
          <Tabs.Indicator />
        </Tabs.List>
        {scrollTabs.map((tab) => (
          <Tabs.Panel key={tab.value} value={tab.value}>
            {tab.label} content
          </Tabs.Panel>
        ))}
      </Tabs.Root>
    </div>
  ),
  play: async ({ canvas }) => {
    const list = canvas.getByRole('tablist')
    const viewport = list.closest<HTMLElement>('[data-id$="-viewport"]')

    await expect(viewport).toBeDefined()
    await expect(viewport?.scrollWidth).toBeGreaterThan(viewport?.clientWidth ?? 0)
    await expect(getComputedStyle(viewport!).overflowY).toBe('hidden')
  },
}

export const HorizontalScrollLastSelected: StoryObj<DefaultArgs> = {
  args: {
    onValueChange: fn(),
  },
  render: (args) => (
    <div style={{ maxWidth: 400 }}>
      <Tabs.Root defaultValue="configuration" onValueChange={args.onValueChange}>
        <Tabs.List>
          {scrollTabs.map((tab) => (
            <Tabs.Tab key={tab.value} value={tab.value}>
              {tab.label}
            </Tabs.Tab>
          ))}
          <Tabs.Indicator />
        </Tabs.List>
        {scrollTabs.map((tab) => (
          <Tabs.Panel key={tab.value} value={tab.value}>
            {tab.label} content
          </Tabs.Panel>
        ))}
      </Tabs.Root>
    </div>
  ),
}
