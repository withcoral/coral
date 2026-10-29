/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { createRoutesStub } from 'react-router'
import { describe, expect, it, vi } from 'vitest'

import { Cell } from './cell'
import { Container } from './container'
import { Row } from './row'
import { useRowCursor } from './row-cursor'
import { RowLink } from './row-link'

function renderRow(onButton = vi.fn(), active = false) {
  const Stub = createRoutesStub([
    {
      path: '/',
      Component: () => (
        <Container columns={[{ label: 'Name' }, { ariaLabel: 'Actions' }]}>
          <Row to="/detail" active={active}>
            <Cell>Linear</Cell>
            <Cell>
              <button type="button" onClick={onButton}>
                Connect
              </button>
            </Cell>
          </Row>
        </Container>
      ),
    },
    { path: '/detail', Component: () => <h1>Detail</h1> },
  ])
  render(<Stub initialEntries={['/']} />)
}

describe('Table Row with a link', () => {
  // A row that only follows its link from the name reads as broken.
  it('follows the link from a click anywhere in the row', async () => {
    renderRow()
    await userEvent.click(screen.getByRole('cell', { name: 'Linear' }))
    expect(await screen.findByRole('heading', { name: 'Detail' })).toBeInTheDocument()
  })

  // Catches: the cursor row's own tabindex read as a control, so a click in it
  // goes nowhere.
  it('follows the link from a click in the cursor row', async () => {
    renderRow(vi.fn(), true)
    await userEvent.click(screen.getByRole('cell', { name: 'Linear' }))
    expect(await screen.findByRole('heading', { name: 'Detail' })).toBeInTheDocument()
  })

  it('leaves a click on a control in the row to the control', async () => {
    const onButton = vi.fn()
    renderRow(onButton)
    await userEvent.click(screen.getByRole('button', { name: 'Connect' }))
    expect(onButton).toHaveBeenCalledOnce()
    expect(screen.queryByRole('heading', { name: 'Detail' })).not.toBeInTheDocument()
  })
})

function CursorTable() {
  const table = useRowCursor({ filterId: 'f' })
  return (
    <>
      <a href="/elsewhere">Sidebar</a>
      <input id="f" aria-label="Filter" />
      <Container ref={table} columns={[{ label: 'Name' }, { ariaLabel: 'Actions' }]}>
        {['linear', 'github'].map((key) => (
          <Row key={key} to={`/${key}`}>
            <Cell>
              <RowLink to={`/${key}`}>{key}</RowLink>
            </Cell>
            <Cell control>
              <button type="button">{`Connect ${key}`}</button>
            </Cell>
          </Row>
        ))}
      </Container>
    </>
  )
}

function renderCursorTable() {
  const Stub = createRoutesStub([
    { path: '/', Component: CursorTable },
    { path: '/github', Component: () => <h1>GitHub</h1> },
  ])
  render(<Stub initialEntries={['/']} />)
  return screen.getByRole('textbox', { name: 'Filter' })
}

describe('Table row cursor', () => {
  // Tab and the arrows must land on the same row, so the arrows move real focus.
  it('moves focus from the filter down the rows, and Enter opens the focused row', async () => {
    await userEvent.click(renderCursorTable())
    await userEvent.keyboard('{ArrowDown}{ArrowDown}')
    expect(screen.getByRole('link', { name: 'github' })).toHaveFocus()
    await userEvent.keyboard('{Enter}')
    expect(await screen.findByRole('heading', { name: 'GitHub' })).toBeInTheDocument()
  })

  // The arrows reach the rows from wherever focus was left, such as the link
  // that opened the page, and step on from a button in a row.
  it.each([
    ['the page', () => {}, 'linear'],
    [
      'a link outside the table',
      () => screen.getByRole('link', { name: 'Sidebar' }).focus(),
      'linear',
    ],
    [
      'a button in a row',
      () => screen.getByRole('button', { name: 'Connect linear' }).focus(),
      'github',
    ],
  ])('moves focus with ↓ from %s', async (_, focus, expected) => {
    renderCursorTable()
    focus()
    await userEvent.keyboard('{ArrowDown}')
    expect(screen.getByRole('link', { name: expected })).toHaveFocus()
  })

  it('returns focus to the filter from the first row', async () => {
    const filter = renderCursorTable()
    await userEvent.click(filter)
    await userEvent.keyboard('{ArrowDown}{ArrowUp}')
    expect(filter).toHaveFocus()
  })

  // A tab panel kept mounted while hidden must not take the arrows from the visible one.
  it('leaves ↓ to the visible table when another table is hidden', async () => {
    const Stub = createRoutesStub([{ path: '/', Component: HiddenAndVisible }])
    render(<Stub initialEntries={['/']} />)
    await userEvent.keyboard('{ArrowDown}')
    expect(screen.getByRole('link', { name: 'visible' })).toHaveFocus()
  })
})

function LinkTable({ name }: { name: string }) {
  const table = useRowCursor()
  return (
    <Container ref={table} columns={[{ label: 'Name' }]}>
      <Row to={`/${name}`}>
        <Cell>
          <RowLink to={`/${name}`}>{name}</RowLink>
        </Cell>
      </Row>
    </Container>
  )
}

function HiddenAndVisible() {
  return (
    <>
      <div hidden>
        <LinkTable name="hidden" />
      </div>
      <LinkTable name="visible" />
    </>
  )
}
