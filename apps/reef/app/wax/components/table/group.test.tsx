/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { Cell } from './cell'
import { Container } from './container'
import { Group, GroupToggle } from './group'
import { Head } from './head'
import { Row } from './row'

const columns = [
  { label: 'Name', width: 'fill' },
  { label: 'Requests', width: 96 },
] as const

describe('Table Group', () => {
  it('exposes one labelled rowgroup per top-level group, and a level on each heading row', () => {
    render(
      <Container columns={columns}>
        <Head />
        <Group label="Provider">
          <Group label="Tools" heading={<Cell fullWidth>Tools</Cell>}>
            <Row>
              <Cell>listIssues</Cell>
              <Cell>12</Cell>
            </Row>
          </Group>
        </Group>
      </Container>,
    )

    // The head and the top-level group; the nested group is presentational.
    expect(screen.getAllByRole('rowgroup')).toHaveLength(2)
    expect(screen.getByRole('rowgroup', { name: 'Provider' })).toBeInTheDocument()
    expect(screen.queryByRole('rowgroup', { name: 'Tools' })).not.toBeInTheDocument()
    const levels = screen
      .getAllByRole('row')
      .map((row) => row.getAttribute('aria-level'))
      .filter(Boolean)
    expect(levels).toEqual(['1', '2'])
  })

  it('forwards headingRow to the heading row, keeping its level', async () => {
    const onClick = vi.fn()
    render(
      <Container columns={columns}>
        <Group
          label="Task"
          heading={<Cell fullWidth>Task</Cell>}
          headingRow={{
            recordKey: 'tasks/one',
            selected: true,
            status: { tone: 'success', label: 'success' },
            onClick,
          }}
        >
          <Row>
            <Cell>child</Cell>
          </Row>
        </Group>
      </Container>,
    )

    const heading = screen.getByRole('row', { name: 'Task', description: 'success' })
    expect(heading).toHaveAttribute('aria-level', '1')
    expect(heading).toHaveAttribute('aria-selected', 'true')
    expect(heading).toHaveAttribute('data-record-key', 'tasks/one')
    expect(heading).toHaveAttribute('data-status', 'success')
    await userEvent.click(heading)
    expect(onClick).toHaveBeenCalledOnce()
  })

  // Catches: a cursor heading that loses its mark to a record heading's own
  // state, or a toggle click that also opens the record.
  it('keeps the cursor mark beside a record heading, and its toggle apart from the row', async () => {
    const onClick = vi.fn()
    const onToggle = vi.fn()
    render(
      <Container columns={columns}>
        <Head />
        <Group
          label="Task"
          active
          heading={
            <Cell fullWidth>
              <GroupToggle collapsed={false} label="Task" onToggle={onToggle} />
              Task
            </Cell>
          }
          headingRow={{ recordKey: 'tasks/one', active: false, onClick }}
        >
          <Row>
            <Cell>child</Cell>
          </Row>
        </Group>
      </Container>,
    )

    expect(screen.getByRole('row', { name: /Task/ })).toHaveAttribute('data-active')
    await userEvent.click(screen.getByRole('button', { name: 'Collapse Task' }))
    expect(onToggle).toHaveBeenCalledOnce()
    expect(onClick).not.toHaveBeenCalled()
  })
})
