/** @vitest-environment happy-dom */
import { act, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Cell } from './cell'
import { Container } from './container'
import { Group } from './group'
import { Head } from './head'
import { Row } from './row'

const columns = [{ label: 'Name', width: 'fill' }] as const
const GROUP_HEIGHT = 200

// happy-dom lays nothing out: each group reports a rect from its place in the
// list and the table's scroll offset. Installed before the first render, since
// the table reads the geometry in its mount effect, and restored whatever the
// test's outcome.
const rect = HTMLElement.prototype.getBoundingClientRect
beforeEach(() => {
  layOut()
})

afterEach(() => {
  HTMLElement.prototype.getBoundingClientRect = rect
})

function layOut() {
  HTMLElement.prototype.getBoundingClientRect = function (this: HTMLElement) {
    if (this.getAttribute('role') === 'table') return { top: 0 } as DOMRect
    // The head is a rowgroup too; only the labelled groups take a slot.
    if (this.getAttribute('role') === 'rowgroup' && this.id) {
      const table = this.closest<HTMLElement>('[role="table"]')!
      const index = [...table.querySelectorAll('[role="rowgroup"][id]')].indexOf(this)
      return { top: 44 + index * GROUP_HEIGHT - (table.scrollTop || 0) } as DOMRect
    }
    return rect.call(this)
  }
}

// Catches: the table reporting a group on every scroll instead of on change,
// or picking the first group below the slot instead of the last one above it.
describe('Table.Container onPinnedGroupChange', () => {
  it('reports the group whose heading holds the slot, once per change', async () => {
    const onPinnedGroupChange = vi.fn()
    render(
      <Container columns={columns} layout="scroll" onPinnedGroupChange={onPinnedGroupChange}>
        <Head />
        {['alpha', 'beta', 'gamma'].map((id) => (
          <Group id={id} key={id} label={id}>
            <Row>
              <Cell>{id}</Cell>
            </Row>
          </Group>
        ))}
      </Container>,
    )
    const table = screen.getByRole('table')

    // On mount the first group starts under the head: it is pinned.
    expect(onPinnedGroupChange.mock.calls).toEqual([['alpha']])

    // Still inside alpha: no report.
    table.scrollTop = GROUP_HEIGHT / 2
    await act(async () => table.dispatchEvent(new Event('scroll')))
    expect(onPinnedGroupChange).toHaveBeenCalledTimes(1)

    // gamma's top reaches the slot: gamma is pinned, beta never was reported
    // between because the scroll jumped past it.
    table.scrollTop = 2 * GROUP_HEIGHT
    await act(async () => table.dispatchEvent(new Event('scroll')))
    expect(onPinnedGroupChange.mock.calls).toEqual([['alpha'], ['gamma']])
  })
})
