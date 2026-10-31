/** @vitest-environment happy-dom */
import { act, render, screen } from '@testing-library/react'
import { useEffect, useRef } from 'react'
import { beforeEach, describe, expect, it } from 'vitest'

import { Cell } from './cell'
import { GROUP_HEADING_HEIGHT_PX, HEADING_HEIGHT_PX } from './constants'
import { Container } from './container'
import { Group } from './group'
import { Head } from './head'
import { Row } from './row'
import { useVirtualRows, VirtualGroups, VirtualRows } from './virtual'

const columns = [{ label: 'Name', width: 'fill' }] as const
const ROW = 40
const VIEWPORT = 200

/** A row's top in the scroll port: the head above the list, plus the rows above it. */
const top = (index: number) => HEADING_HEIGHT_PX.default + index * ROW

// happy-dom lays nothing out. TanStack Virtual reads the scroll port and each
// mounted row through `offsetHeight`, so the port is as tall as the viewport
// here and every row measures `ROW`.
function Catalog({
  groups,
  perGroup,
  onHandle,
}: {
  groups: number
  perGroup: number
  onHandle?: (handle: ReturnType<typeof useVirtualRows>) => void
}) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const rows = Array.from({ length: groups }, (_, group) => ({
    label: `Group ${group}`,
    rows: Array.from({ length: perGroup }, (_member, member) => ({
      index: group * (perGroup + 1) + member + 1,
      name: `row-${group}-${member}`,
    })),
  }))
  const virtual = useVirtualRows({
    count: groups * (perGroup + 1),
    estimateSize: () => ROW,
    overscan: 0,
    scrollRef,
  })
  useEffect(() => {
    onHandle?.(virtual)
  }, [onHandle, virtual])
  return (
    <Container columns={columns} layout="scroll" ref={scrollRef}>
      <Head />
      {rows.map((group) => (
        <Group key={group.label} label={group.label}>
          <VirtualRows rows={group.rows} virtual={virtual}>
            {({ index, name }) => (
              <Row key={name} data-index={index} ref={virtual.measureRow}>
                <Cell>{name}</Cell>
              </Row>
            )}
          </VirtualRows>
        </Group>
      ))}
    </Container>
  )
}

// Groups inside one group, as a provider holds its tool groups.
function NestedCatalog({ groups, perGroup }: { groups: number; perGroup: number }) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const list = Array.from({ length: groups }, (_, group) => {
    const index = group * (perGroup + 1) + 1
    return {
      label: `Group ${group}`,
      index,
      end: index + perGroup,
      rows: Array.from({ length: perGroup }, (_row, member) => ({
        index: index + member + 1,
        name: `row-${group}-${member}`,
      })),
    }
  })
  const virtual = useVirtualRows({
    count: groups * (perGroup + 1) + 1,
    estimateSize: () => ROW,
    overscan: 0,
    scrollRef,
  })
  return (
    <Container columns={columns} layout="scroll" ref={scrollRef}>
      <Head />
      <Group label="Provider" index={0}>
        <VirtualGroups groups={list} virtual={virtual}>
          {(group) => (
            <Group key={group.label} label={group.label} index={group.index}>
              <VirtualRows rows={group.rows} virtual={virtual}>
                {({ index, name }) => (
                  <Row key={name} data-index={index} ref={virtual.measureRow}>
                    <Cell>{name}</Cell>
                  </Row>
                )}
              </VirtualRows>
            </Group>
          )}
        </VirtualGroups>
      </Group>
    </Container>
  )
}

describe('Table virtual rows', () => {
  beforeEach(() => {
    Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
      configurable: true,
      get() {
        if (this.getAttribute('role') === 'table') return VIEWPORT
        return this.hasAttribute('data-index') ? ROW : 0
      },
    })
    Object.defineProperty(HTMLElement.prototype, 'offsetWidth', {
      configurable: true,
      get() {
        return 600
      },
    })
  })

  it('mounts only the rows near the viewport and keeps every group heading', () => {
    const { container: host } = render(<Catalog groups={4} perGroup={10} />)
    const mounted = host.querySelectorAll('[role="row"][data-index]')
    // The heading and four rows fill 200px. The total height is covered below.
    expect(mounted).toHaveLength(4)
    expect([...mounted].map((row) => row.textContent)).toEqual([
      'row-0-0',
      'row-0-1',
      'row-0-2',
      'row-0-3',
    ])
    // Every group heading stays mounted, whatever the window holds.
    expect(host.querySelectorAll('[role="row"][aria-level]')).toHaveLength(4)
  })

  // Catches: every heading of a large catalog staying mounted, where the
  // browser lays each one out again on any change.
  it('mounts only the nested groups near the viewport, at their full height', async () => {
    const { container: host } = render(<NestedCatalog groups={40} perGroup={5} />)
    const headings = () =>
      [...host.querySelectorAll('[role="row"][aria-level="2"]')].map((row) => row.textContent)
    const rowsAndSpacers = () =>
      [...host.querySelectorAll<HTMLElement>('[role="row"]')]
        .filter((row) => row.style.blockSize || row.hasAttribute('data-index'))
        .reduce((sum, row) => sum + (parseInt(row.style.blockSize) || ROW), 0)
    expect(headings()).toEqual(['Group 0'])
    expect(rowsAndSpacers()).toBe((40 * 6 + 1) * ROW)

    const table = screen.getByRole('table')
    await act(async () => {
      table.scrollTop = 60 * ROW
      table.dispatchEvent(new Event('scroll'))
    })
    // The first row's heading scrolled out above it, and still mounts to pin.
    const firstRow = host.querySelector('[role="row"][data-index]:not([aria-level])')
    const [, group] = /^row-(\d+)-/.exec(firstRow!.textContent)!
    expect(headings()[0]).toBe(`Group ${group}`)
    expect(headings().length).toBeLessThanOrEqual(2)
    expect(rowsAndSpacers()).toBe((40 * 6 + 1) * ROW)
  })

  it('keeps the total height whichever rows are mounted', async () => {
    const { container: host } = render(<Catalog groups={3} perGroup={6} />)
    const heights = () =>
      [...host.querySelectorAll<HTMLElement>('[role="row"]')]
        .filter((row) => row.style.blockSize || row.hasAttribute('data-index'))
        .reduce((sum, row) => sum + (parseInt(row.style.blockSize) || ROW), 0)
    // The catalog spans 18 rows, before and after scrolling.
    expect(heights()).toBe(18 * ROW)
    const table = screen.getByRole('table')
    await act(async () => {
      table.scrollTop = 8 * ROW
      table.dispatchEvent(new Event('scroll'))
    })
    expect(heights()).toBe(18 * ROW)
    const mounted = [...host.querySelectorAll('[role="row"][data-index]')].map(
      (row) => row.textContent,
    )
    expect(mounted[0]).not.toBe('row-0-0')
  })

  // Catches: a revealed row landing at the scroll port's top edge, under the
  // pinned head and group heading, where the reader cannot see it; or one
  // head lower, so the heading above it keeps the pinned slot.
  it('scrolls a row to sit under the headings pinned above it', async () => {
    let handle: ReturnType<typeof useVirtualRows> | null = null
    render(
      <Catalog
        groups={3}
        perGroup={6}
        onHandle={(next) => {
          handle = next
        }}
      />,
    )
    const table = screen.getByRole('table')
    const scrolls: number[] = []
    table.scrollTo = ((options: ScrollToOptions) =>
      scrolls.push(options.top!)) as typeof table.scrollTo
    // happy-dom reports no scroll height; the virtualizer clamps to it.
    Object.defineProperty(table, 'scrollHeight', { get: () => 21 * ROW })
    Object.defineProperty(table, 'clientHeight', { get: () => VIEWPORT })
    // row-1-2 is index 10: 10 rows above it.
    await act(async () => handle!.scrollToIndex(10, { depth: 1 }))
    expect(scrolls).toEqual([top(10) - HEADING_HEIGHT_PX.default - GROUP_HEADING_HEIGHT_PX.default])
    // A top-level heading sits under the head alone.
    await act(async () => handle!.scrollToIndex(7))
    expect(scrolls.at(-1)).toBe(top(7) - HEADING_HEIGHT_PX.default)
    // A row below the port scrolls only until its bottom edge shows.
    await act(async () => handle!.revealIndex(6, { depth: 1 }))
    expect(scrolls.at(-1)).toBe(top(7) - VIEWPORT)
  })
})
