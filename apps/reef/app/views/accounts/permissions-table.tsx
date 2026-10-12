import { useDeferredValue, useState } from 'react'
import { CatalogToolbar } from '@/components/catalog'
import { Button, Menu, Table } from '@/wax/components'
import type { IconName } from '@/wax/components/icon'
import * as styles from './accounts.css'

type PermissionStatus = 'granted' | 'missing' | 'extra'

export interface PermissionRow {
  name: string
  /** Absent before a grant exists: the row is only a requested permission. */
  status?: PermissionStatus
}

/** A short list reads at a glance; a filter only helps a long one. */
const FILTER_FROM = 8

// A grant beyond the request still reads as granted: it is what the account can do.
const ORDER: Record<PermissionStatus, number> = { missing: 0, granted: 1, extra: 1 }

/**
 * Joins the requested permissions with the grant the service reported. Services
 * separate a grant with spaces (RFC 6749) or commas (Slack), so both split it.
 * Missing permissions sort first, since they explain a failing call.
 */
export function permissionRows(requested: string[], grantedScope?: string): PermissionRow[] {
  if (grantedScope === undefined) {
    return [...new Set(requested)].toSorted().map((name) => ({ name }))
  }
  const granted = new Set(grantedScope.split(/[\s,]+/).filter(Boolean))
  const wanted = new Set(requested)
  const rows: PermissionRow[] = [
    ...[...wanted].map((name) => ({
      name,
      status: (granted.has(name) ? 'granted' : 'missing') as PermissionStatus,
    })),
    ...[...granted]
      .filter((name) => !wanted.has(name))
      .map((name) => ({ name, status: 'extra' as const })),
  ]
  return rows.toSorted(
    (a, b) => ORDER[a.status!] - ORDER[b.status!] || a.name.localeCompare(b.name),
  )
}

/** One sentence that counts the grant against the request. */
export function permissionSummary(rows: PermissionRow[]): string {
  const requested = rows.filter((r) => r.status !== 'extra').length
  if (rows.every((r) => r.status === undefined)) {
    return `Coral asks for ${requested} ${requested === 1 ? 'permission' : 'permissions'}.`
  }
  const granted = rows.filter((r) => r.status === 'granted').length
  return `The service granted ${granted} of ${requested} requested permissions.`
}

type StatusFilter = 'all' | 'missing' | 'granted'

const STATUS_FILTERS: Record<StatusFilter, { label: string; icon: IconName }> = {
  all: { label: 'All statuses', icon: 'ListFilter' },
  missing: { label: 'Not granted', icon: 'CircleAlert' },
  granted: { label: 'Granted', icon: 'CircleCheck' },
}

/** About ten compact rows; the rest scroll under the heading. */
const MAX_HEIGHT = 360

export function PermissionsTable({ rows }: { rows: PermissionRow[] }) {
  const [input, setInput] = useState('')
  const [status, setStatus] = useState<StatusFilter>('all')
  const query = useDeferredValue(input).trim()
  const filter = query.toLowerCase()
  const missing = rows.filter((r) => r.status === 'missing').length
  // A status choice helps only when the grant left something out.
  const byStatus = missing > 0
  const shown = rows.filter(
    (r) =>
      r.name.toLowerCase().includes(filter) &&
      (!byStatus || status === 'all' || (status === 'missing') === (r.status === 'missing')),
  )
  const withStatus = rows.some((r) => r.status)
  return (
    <div className={styles.permissions}>
      {(byStatus || rows.length >= FILTER_FROM) && (
        <CatalogToolbar
          filter={{ label: 'Filter permissions', value: input, onChange: setInput, fill: true }}
        >
          {byStatus && (
            <StatusFilterMenu
              value={status}
              onChange={setStatus}
              counts={{ all: rows.length, missing, granted: rows.length - missing }}
            />
          )}
        </CatalogToolbar>
      )}
      <Table.Container
        ariaLabel="Permissions"
        variant="card"
        density="compact"
        layout="fixed"
        maxHeight={MAX_HEIGHT}
        staticRows
        columns={[
          { label: 'Permission', width: 'minmax(160px, 1fr)' },
          ...(withStatus ? [{ label: 'Status', width: 'minmax(120px, 0.6fr)' }] : []),
        ]}
      >
        <Table.Head />
        {shown.length === 0 ? (
          <Table.Status>
            {query ? `No permissions match “${query}”.` : 'No permissions match.'}
          </Table.Status>
        ) : (
          <Table.Body>
            {shown.map((row) => (
              <Table.Row key={row.name}>
                <Table.Cell mono>{row.name}</Table.Cell>
                {withStatus && (
                  <Table.Cell>{row.status && <StatusLabel status={row.status} />}</Table.Cell>
                )}
              </Table.Row>
            ))}
          </Table.Body>
        )}
      </Table.Container>
    </div>
  )
}

function StatusFilterMenu({
  value,
  counts,
  onChange,
}: {
  value: StatusFilter
  counts: Record<StatusFilter, number>
  onChange: (value: StatusFilter) => void
}) {
  const active = STATUS_FILTERS[value]
  return (
    <Menu.Container>
      <Menu.Trigger
        render={<Button.Container variant="secondary" ariaLabel="Filter permissions by status" />}
      >
        <Button.Icon name={active.icon} />
        <Button.Text>{active.label}</Button.Text>
        <Button.Icon name="ChevronDown" />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.RadioGroup
          value={value}
          onValueChange={(next: string) => onChange(next as StatusFilter)}
        >
          {(Object.keys(STATUS_FILTERS) as StatusFilter[]).map((key) => (
            <Menu.RadioItem key={key} value={key} iconName={STATUS_FILTERS[key].icon}>
              {`${STATUS_FILTERS[key].label} (${counts[key]})`}
            </Menu.RadioItem>
          ))}
        </Menu.RadioGroup>
      </Menu.Content>
    </Menu.Container>
  )
}

/** The words carry the status; red only helps a missing permission stand out. */
function StatusLabel({ status }: { status: PermissionStatus }) {
  if (status !== 'missing') return 'Granted'
  return <span className={styles.missing}>Not granted</span>
}
