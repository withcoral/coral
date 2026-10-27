import { useDeferredValue, useMemo, useState, type ReactNode } from 'react'
import { Link } from 'react-router'
import { CatalogLayout, CatalogRail, CatalogRailItem, CatalogToolbar } from '@/components/catalog'
import { CopyMcpEndpoint } from '@/components/copy-mcp-endpoint'
import { EmptyPage } from '@/components/empty-page'
import { PolicyToggle } from '@/components/policy-toggle'
import { Button, Table } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import { Tooltip } from '@/wax/components/tooltip'
import { trail } from '@/views/management/layout'
import { useArrivalToast } from '@/lib/use-arrival-toast'
import { filterQuery } from '@/utils/filter-query'
import type { WorkspaceView } from '@/views/management/types'
import type { SavedFunctionSummaryView } from './types'
import * as styles from './functions.css'

/** Deleting a function returns to the list with this flag in the query. */
export const FUNCTION_DELETED = ['functionDeleted'] as const

export interface FunctionsPageProps {
  workspace: WorkspaceView
  functions: SavedFunctionSummaryView[]
  /** The open function's drawer, when the address names one. */
  children?: ReactNode
}

/** Functions whose path starts with one segment, such as `linear` for `linear.completedSince`. */
interface Namespace {
  name: string
  domId: string
  /** Each function with its name inside the namespace. */
  members: Member[]
}

interface Member {
  summary: SavedFunctionSummaryView
  /** The path after the namespace, or the whole path for a top-level function. */
  name: string
}

/** Splits top-level functions from namespaced ones, keeping path order within each. */
function catalog(functions: SavedFunctionSummaryView[]) {
  const topLevel: Member[] = []
  const byName = new Map<string, Namespace>()
  for (const summary of functions) {
    const dot = summary.path.indexOf('.')
    if (dot === -1) {
      topLevel.push({ summary, name: summary.path })
      continue
    }
    const name = summary.path.slice(0, dot)
    const namespace = byName.get(name) ?? { name, domId: `function-namespace-${name}`, members: [] }
    namespace.members.push({ summary, name: summary.path.slice(dot + 1) })
    byName.set(name, namespace)
  }
  const namespaces = [...byName.values()].toSorted((a, b) => a.name.localeCompare(b.name))
  return { topLevel, namespaces }
}

/** A row in a namespace lines its name up under the group label, one step in. */
function FunctionRow({ member: { summary, name }, grouped }: { member: Member; grouped: boolean }) {
  return (
    <Table.Row to={summary.href} preventScrollReset>
      <Table.Cell>
        <span className={grouped ? styles.groupedName : styles.name}>
          <Button.Container
            as={Link}
            to={summary.href}
            prefetch="intent"
            preventScrollReset
            variant="link"
            size="32"
          >
            <Button.Text tooltip={name}>{name}</Button.Text>
          </Button.Container>
        </span>
      </Table.Cell>
      <Table.Cell control>
        <PolicyPreview name={summary.path} />
      </Table.Cell>
    </Table.Row>
  )
}

/** Every function is always allowed until Coral supports function policy. */
function PolicyPreview({ name }: { name: string }) {
  return (
    <Tooltip content="Function policy is not available yet. Coral runs every function you save.">
      <span className={styles.policy} tabIndex={0}>
        <PolicyToggle disabled label={`Policy for ${name}`} value="enabled" />
      </span>
    </Tooltip>
  )
}

/** The current User's own Saved Functions in one Workspace. Reef only reads them. */
export function FunctionsPage({ workspace: w, functions, children }: FunctionsPageProps) {
  useArrivalToast(FUNCTION_DELETED, () => ({ title: 'Function deleted' }))
  const [railFilterInput, setRailFilterInput] = useState('')
  const [tableFilterInput, setTableFilterInput] = useState('')
  const [current, setCurrent] = useState<string | null>(null)
  const railQuery = filterQuery(useDeferredValue(railFilterInput))
  const tableQuery = filterQuery(useDeferredValue(tableFilterInput))
  const { topLevel, namespaces } = useMemo(() => catalog(functions), [functions])
  const matches = (m: Member) => tableQuery.matches(m.summary.path)
  const listed = namespaces.filter((n) => railQuery.matches(n.name))
  // A namespace filter that leaves some out narrows the table to the rest,
  // which leaves out top-level functions: they belong to none.
  const shownTopLevel = listed.length < namespaces.length ? [] : topLevel.filter(matches)
  const shown = listed
    .map((n) => ({ ...n, members: n.members.filter(matches) }))
    .filter((n) => n.members.length > 0)
  const listPath = `${w.path}/functions`
  return (
    <PageLayout
      variant="split"
      breadcrumbs={trail([], 'Functions')}
      titled={functions.length > 0}
      compact
    >
      {functions.length === 0 ? (
        <div className={styles.empty}>
          <EmptyPage
            iconName="SquareFunction"
            title="No functions yet"
            description="Save a function from an MCP client. Your functions in this workspace show here."
            action={<CopyMcpEndpoint endpoint={w.connectEndpoint} />}
          />
        </div>
      ) : (
        <CatalogLayout
          rail={
            <CatalogRail
              label="function namespaces"
              filter={{
                label: 'Filter namespaces',
                value: railFilterInput,
                onChange: setRailFilterInput,
              }}
              empty={
                listed.length > 0
                  ? null
                  : {
                      message: railQuery.text
                        ? `No namespaces match “${railQuery.text}”`
                        : 'No namespaces yet.',
                    }
              }
            >
              {listed.map((n) => (
                <CatalogRailItem
                  key={n.domId}
                  to={`${listPath}#${n.domId}`}
                  current={n.domId === current}
                  onSelect={() => {
                    setCurrent(n.domId)
                    document.getElementById(n.domId)?.scrollIntoView({ block: 'start' })
                  }}
                  name={n.name}
                  count={n.members.length}
                />
              ))}
            </CatalogRail>
          }
        >
          <CatalogToolbar
            title="Functions"
            filter={{
              label: 'Filter functions',
              value: tableFilterInput,
              onChange: setTableFilterInput,
            }}
          />
          <Table.Container
            ariaLabel="Functions"
            layout="scroll"
            className={styles.table}
            onPinnedGroupChange={setCurrent}
            columns={[
              { label: 'Function name', width: 'minmax(280px, 1fr)' },
              { label: 'Policy', width: 'minmax(180px, 0.4fr)' },
            ]}
          >
            <Table.Head />
            {shownTopLevel.length === 0 && shown.length === 0 && (
              <Table.Status>No functions match.</Table.Status>
            )}
            {shownTopLevel.length > 0 && (
              <Table.Body>
                {shownTopLevel.map((m) => (
                  <FunctionRow key={m.summary.path} member={m} grouped={false} />
                ))}
              </Table.Body>
            )}
            {shown.map((n) => (
              <Table.Group key={n.domId} id={n.domId} label={n.name}>
                {n.members.map((m) => (
                  <FunctionRow key={m.summary.path} member={m} grouped />
                ))}
              </Table.Group>
            ))}
          </Table.Container>
        </CatalogLayout>
      )}
      {children}
    </PageLayout>
  )
}
