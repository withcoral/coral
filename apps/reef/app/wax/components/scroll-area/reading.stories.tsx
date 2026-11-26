import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect, userEvent, waitFor, within } from 'storybook/test'

import { Button } from '@/wax/components'
import { Container } from './scroll-area'

const meta: Meta<typeof Container> = {
  component: Container,
  title: 'Wax/ScrollArea/Reading',
}
export default meta
type Story = StoryObj<typeof Container>

function ReadingExample() {
  const [grown, grow] = useState(false)
  const [removed, remove] = useState(false)
  const [paused, pause] = useState(false)
  const [hidden, hide] = useState(false)
  const [scope, setScope] = useState('first')
  const [inserted, insert] = useState(0)
  const [generation, replace] = useState(0)
  const [follow, setFollow] = useState(true)
  const [reveal, setReveal] = useState<string>()
  const [lost, setLost] = useState(0)
  return (
    <>
      <div style={{ display: 'flex', flexWrap: 'wrap' }}>
        <Button.TextButton onClick={() => grow(true)}>Grow earlier record</Button.TextButton>
        <Button.TextButton onClick={() => remove(true)}>Remove visible anchor</Button.TextButton>
        <Button.TextButton onClick={() => pause(!paused)}>Toggle connection</Button.TextButton>
        <Button.TextButton onClick={() => hide(!hidden)}>Toggle visibility</Button.TextButton>
        <Button.TextButton onClick={() => setScope(scope === 'first' ? 'second' : 'first')}>
          Switch scope
        </Button.TextButton>
        <Button.TextButton onClick={() => insert(inserted + 1)}>Prepend record</Button.TextButton>
        <Button.TextButton onClick={() => setFollow(false)}>Leave global edge</Button.TextButton>
        <Button.TextButton onClick={() => setReveal(`${generation}:25`)}>
          Reveal record
        </Button.TextButton>
        <Button.TextButton onClick={() => replace(generation + 1)}>
          Replace records
        </Button.TextButton>
      </div>
      <output aria-label="Lost position">{lost}</output>
      <output aria-label="Following">{String(follow)}</output>
      <div style={{ display: hidden ? 'none' : 'block' }}>
        <Container
          height="240px"
          width="300px"
          fade="none"
          scrollDirection="vertical"
          viewportClassName="reading-records"
          readingKey={scope}
          readingPaused={paused}
          readingFollowStart={follow}
          onReadingStartChange={setFollow}
          onReadingPositionLost={() => setLost((value) => value + 1)}
          revealRecord={reveal}
        >
          {Array.from({ length: paused ? 0 : 30 + inserted }, (_, index) => index - inserted)
            .filter((index) => !removed || index !== 10)
            .map((index) => (
              <article
                key={`${generation}:${index}`}
                data-record-key={`${generation}:${index}`}
                style={{ height: grown && index === 0 ? 160 : index % 2 ? 48 : 32 }}
              >
                Record {index}
              </article>
            ))}
        </Container>
      </div>
    </>
  )
}

function readingPort(canvasElement: HTMLElement) {
  return canvasElement.querySelector<HTMLElement>('.reading-records')!
}
function record(canvasElement: HTMLElement, key: string) {
  return canvasElement.querySelector<HTMLElement>(`[data-record-key="${key}"]`)!
}
function offset(port: HTMLElement, anchor: HTMLElement) {
  return anchor.getBoundingClientRect().top - port.getBoundingClientRect().top
}
function scrollToRecord(port: HTMLElement, anchor: HTMLElement) {
  port.scrollTop += offset(port, anchor)
  port.dispatchEvent(new Event('scroll'))
}

/** Ordinary articles keep identity and pixel offset through changes before, at, and outside the viewport. */
export const LiveRecords: Story = {
  render: () => <ReadingExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = readingPort(canvasElement)
    scrollToRecord(port, record(canvasElement, '0:10'))
    const before = offset(port, record(canvasElement, '0:11'))
    for (const name of ['Grow earlier record', 'Remove visible anchor']) {
      await userEvent.click(canvas.getByRole('button', { name }))
      await waitFor(() =>
        expect(Math.abs(offset(port, record(canvasElement, '0:11')) - before)).toBeLessThan(1),
      )
    }
    for (const name of ['Toggle visibility', 'Toggle connection']) {
      await userEvent.click(canvas.getByRole('button', { name }))
      await userEvent.click(canvas.getByRole('button', { name }))
      await waitFor(() =>
        expect(Math.abs(offset(port, record(canvasElement, '0:11')) - before)).toBeLessThan(1),
      )
    }
    expect(canvas.queryByRole('table')).toBeNull()
  },
}

/** Navigation keeps separate positions, reveal runs once, and removing every anchor reports loss. */
export const NavigationAndReveal: Story = {
  render: () => <ReadingExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = readingPort(canvasElement)
    scrollToRecord(port, record(canvasElement, '0:10'))
    const before = offset(port, record(canvasElement, '0:10'))
    await userEvent.click(canvas.getByRole('button', { name: 'Switch scope' }))
    await waitFor(() => expect(port.scrollTop).toBe(0))
    await userEvent.click(canvas.getByRole('button', { name: 'Switch scope' }))
    await waitFor(() =>
      expect(Math.abs(offset(port, record(canvasElement, '0:10')) - before)).toBeLessThan(1),
    )
    await userEvent.click(canvas.getByRole('button', { name: 'Reveal record' }))
    await waitFor(() => {
      const target = record(canvasElement, '0:25')
      expect(offset(port, target)).toBeGreaterThanOrEqual(0)
      expect(offset(port, target) + target.offsetHeight).toBeLessThanOrEqual(port.clientHeight)
    })
    scrollToRecord(port, record(canvasElement, '0:10'))
    await userEvent.click(canvas.getByRole('button', { name: 'Grow earlier record' }))
    await waitFor(() =>
      expect(Math.abs(offset(port, record(canvasElement, '0:10')))).toBeLessThan(1),
    )
    await userEvent.click(canvas.getByRole('button', { name: 'Replace records' }))
    await waitFor(() => expect(canvas.getByLabelText('Lost position').textContent).toBe('1'))
  },
}

/** A local top does not certify that the caller is reading the global edge. */
export const FollowGlobalEdge: Story = {
  render: () => <ReadingExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = readingPort(canvasElement)
    await userEvent.click(canvas.getByRole('button', { name: 'Prepend record' }))
    await waitFor(() => expect(port.scrollTop).toBe(0))
    await userEvent.click(canvas.getByRole('button', { name: 'Leave global edge' }))
    const anchor = record(canvasElement, '0:-1')
    const before = offset(port, anchor)
    await userEvent.click(canvas.getByRole('button', { name: 'Prepend record' }))
    await waitFor(() => {
      expect(port.scrollTop).toBeGreaterThan(0)
      expect(Math.abs(offset(port, anchor) - before)).toBeLessThan(1)
    })
    expect(canvas.getByLabelText('Following').textContent).toBe('false')
  },
}
