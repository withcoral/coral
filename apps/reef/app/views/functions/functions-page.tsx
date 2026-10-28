import {
  createContext,
  useContext,
  useDeferredValue,
  useMemo,
  useState,
  type ReactNode,
} from 'react'
import { useNavigate, useParams } from 'react-router'
import classNames from 'classnames'
import { CatalogLayout, CatalogRail, CatalogRailItem, CatalogToolbar } from '@/components/catalog'
import { CopyMcpEndpoint } from '@/components/copy-mcp-endpoint'
import { EmptyPage } from '@/components/empty-page'
import { PolicyToggle } from '@/components/policy-toggle'
import { Button, Table, Typography } from '@/wax/components'
import { HighlightedMatches } from '@/wax/components/highlighted-matches'
import { Icon } from '@/wax/components/icon'
import { PageLayout } from '@/wax/components/page-layout'
import { Tooltip } from '@/wax/components/tooltip'
import { trail } from '@/views/management/layout'
import { useArrivalToast } from '@/lib/use-arrival-toast'
import { countLabel } from '@/utils/count-label'
import { filterQuery, type FilterQuery } from '@/utils/filter-query'
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

/** The namespace a function path starts with, or null for a top-level function. */
function namespaceOf(path: string) {
  const dot = path.indexOf('.')
  return dot === -1 ? null : path.slice(0, dot)
}

/** Splits top-level functions from namespaced ones, keeping path order within each. */
function catalog(functions: SavedFunctionSummaryView[]) {
  const topLevel: Member[] = []
  const byName = new Map<string, Namespace>()
  for (const summary of functions) {
    const name = namespaceOf(summary.path)
    if (name === null) {
      topLevel.push({ summary, name: summary.path })
      continue
    }
    const namespace = byName.get(name) ?? { name, domId: `function-namespace-${name}`, members: [] }
    namespace.members.push({ summary, name: summary.path.slice(name.length + 1) })
    byName.set(name, namespace)
  }
  const namespaces = [...byName.values()].toSorted((a, b) => a.name.localeCompare(b.name))
  return { topLevel, namespaces }
}

/** A row opens its function's drawer; the name lines up under the column heading. */
function FunctionRow({ member: { summary, name }, query }: { member: Member; query: FilterQuery }) {
  return (
    <Table.Row to={summary.href} preventScrollReset>
      <span role="cell" className={styles.nameCell}>
        <Table.RowLink to={summary.href}>
          <Tooltip content={name} showOnlyWhenTruncated>
            <span className={styles.name}>
              <HighlightedMatches text={name} query={query} />
            </span>
          </Tooltip>
        </Table.RowLink>
      </span>
      <span role="cell" className={styles.policyCell}>
        <PolicyPreview name={summary.path} />
      </span>
    </Table.Row>
  )
}

/** Every function is always allowed until Coral supports function policy. */
function PolicyPreview({ name }: { name: string }) {
  return (
    <Tooltip content="Function policy is not available yet. Coral runs every function you save.">
      <span className={styles.policy} tabIndex={0}>
        <PolicyToggle size="small" disabled label={`Policy for ${name}`} value="enabled" />
      </span>
    </Tooltip>
  )
}

const FILTER_ID = 'function-filter'

// Where an open drawer returns: the address of the tab under it.
const TabPath = createContext<string | null>(null)
/** The address of the tab the drawer opened over, or null outside the page. */
export const useTabPath = () => useContext(TabPath)

/** The current User's own Saved Functions in one Workspace. Reef only reads them. */
export function FunctionsPage({ workspace: w, functions, children }: FunctionsPageProps) {
  useArrivalToast(FUNCTION_DELETED, () => ({ title: 'Function deleted' }))
  const [railFilterInput, setRailFilterInput] = useState('')
  const [tableFilterInput, setTableFilterInput] = useState('')
  const navigate = useNavigate()
  const params = useParams()
  // The rail is a set of tabs: the table shows one namespace, or every one. A
  // namespace route picks the tab. A function route opens the drawer over the
  // tab the reader is on when that tab lists it; otherwise it opens its
  // namespace's tab.
  const routeNamespace = params.namespace ?? null
  const openNamespace = params.path === undefined ? undefined : namespaceOf(params.path)
  const [lastTab, setLastTab] = useState(openNamespace ?? routeNamespace)
  const keepsTab = openNamespace !== undefined && (lastTab === null || lastTab === openNamespace)
  const routeTab = keepsTab ? undefined : (openNamespace ?? routeNamespace)
  if (routeTab !== undefined && routeTab !== lastTab) setLastTab(routeTab)
  const selectedTab = routeTab === undefined ? lastTab : routeTab
  // The namespaces the reader collapsed. A filter leaves them collapsed.
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set())
  const table = Table.useRowCursor({ filterId: FILTER_ID })
  const railQuery = filterQuery(useDeferredValue(railFilterInput))
  const tableQuery = filterQuery(useDeferredValue(tableFilterInput))
  const { topLevel, namespaces } = useMemo(() => catalog(functions), [functions])
  // A tab whose namespace lost its last function falls back to All.
  const tab = namespaces.some((n) => n.name === selectedTab) ? selectedTab : null
  // The filter matches only the name the row shows, so every match is visible.
  const matches = (m: Member) => tableQuery.matches(m.name)
  // The rail filter narrows the rail only, not the table.
  const listed = namespaces.filter((n) => railQuery.matches(n.name))
  // Top-level functions belong to no namespace, so only the All tab shows them.
  const shownTopLevel = tab === null ? topLevel.filter(matches) : []
  const shown = namespaces
    .filter((n) => tab === null || n.name === tab)
    .map((n) => ({ ...n, members: n.members.filter(matches) }))
    .filter((n) => n.members.length > 0)
  const anyExpanded = shown.some((n) => !collapsed.has(n.name))
  const toggleCollapsed = (name: string) =>
    setCollapsed((current) => {
      const next = new Set(current)
      if (!next.delete(name)) next.add(name)
      return next
    })
  const toggleAllCollapsed = () => {
    setCollapsed((current) => {
      const next = new Set(current)
      for (const n of shown) {
        if (anyExpanded) next.add(n.name)
        else next.delete(n.name)
      }
      return next
    })
    table.current?.scrollTo?.({ top: 0 })
  }
  const listPath = `${w.path}/functions`
  const tabPath = (name: string | null) =>
    name === null ? listPath : `${listPath}/namespaces/${encodeURIComponent(name)}`
  // A tab's address names it, so the URL can be shared. Each tab pushes an
  // entry, so Back returns to the tab before it.
  const openTab = (name: string | null) => {
    navigate(tabPath(name), { preventScrollReset: true })
    table.current?.scrollTo?.({ top: 0 })
  }
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
                listed.length > 0 || namespaces.length === 0
                  ? null
                  : { message: `No namespaces match “${railQuery.text}”` }
              }
            >
              <CatalogRailItem
                to={listPath}
                current={tab === null}
                onSelect={() => openTab(null)}
                leading={<Icon name="Layers" size="20" color="secondary" />}
                name="All namespaces"
                count={functions.length}
              />
              {listed.map((n) => (
                <CatalogRailItem
                  key={n.domId}
                  to={tabPath(n.name)}
                  current={n.name === tab}
                  onSelect={() => openTab(n.name)}
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
              id: FILTER_ID,
              label: 'Filter functions',
              shortcut: '$mod+f',
              value: tableFilterInput,
              onChange: setTableFilterInput,
            }}
          />
          <Table.Container
            ref={table}
            ariaLabel="Functions"
            density="comfortable"
            groupHeadings="prominent"
            layout="scroll"
            className={styles.table}
            columns={[{ width: 'minmax(280px, 1fr)' }, { width: 'max-content' }]}
          >
            <Table.Head>
              <Table.Row>
                <span
                  role="columnheader"
                  className={classNames(styles.nameHeader, Table.groupToggleHeading)}
                >
                  <Button.IconButton
                    variant="bare"
                    size="22"
                    name={anyExpanded ? 'ChevronsDownUp' : 'ChevronsUpDown'}
                    tooltipText={anyExpanded ? 'Collapse all' : 'Expand all'}
                    disabled={shown.length === 0}
                    onClick={toggleAllCollapsed}
                  />
                  Function name
                </span>
                <span role="columnheader" className={styles.policyHeader}>
                  Policy
                </span>
              </Table.Row>
            </Table.Head>
            {shownTopLevel.length === 0 && shown.length === 0 && (
              <Table.Status>No functions match.</Table.Status>
            )}
            {shownTopLevel.length > 0 && (
              <Table.Body>
                {shownTopLevel.map((m) => (
                  <FunctionRow key={m.summary.path} member={m} query={tableQuery} />
                ))}
              </Table.Body>
            )}
            {shown.map((n) => (
              <Table.Group
                key={n.domId}
                id={n.domId}
                label={n.name}
                collapsed={collapsed.has(n.name)}
                heading={
                  <span role="cell" className={styles.groupLabel}>
                    <Table.GroupToggle
                      collapsed={collapsed.has(n.name)}
                      label={n.name}
                      onToggle={() => toggleCollapsed(n.name)}
                    />
                    <Typography.BodyStrong as="span">{n.name}</Typography.BodyStrong>
                    <Typography.BodySmall as="span" variant="tertiary">
                      {countLabel(n.members.length, 'function')}
                    </Typography.BodySmall>
                  </span>
                }
              >
                {n.members.map((m) => (
                  <FunctionRow key={m.summary.path} member={m} query={tableQuery} />
                ))}
              </Table.Group>
            ))}
          </Table.Container>
        </CatalogLayout>
      )}
      <TabPath value={tabPath(tab)}>{children}</TabPath>
    </PageLayout>
  )
}
