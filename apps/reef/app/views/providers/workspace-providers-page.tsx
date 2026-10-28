import classNames from 'classnames'
import type { MouseEvent, ReactNode, Ref } from 'react'
import { memo, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router'

import { ActionBar, Banner, Button, Dialog, Inputs, Menu, Typography } from '@/wax/components'
import { animations } from '@/wax/animations'
import { CodeChip } from '@/wax/components/code-chip'
import { Icon, type IconName } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { HighlightedMatches } from '@/wax/components/highlighted-matches'
import { Logo } from '@/wax/components/logo'
import { Markdown } from '@/wax/components/markdown'
import { PageLayout } from '@/wax/components/page-layout'
import { Picker } from '@/wax/components/picker'
import { Pill, type PillColor } from '@/wax/components/pill'
import { StatusDot } from '@/wax/components/status'
import { InfoTip, Tooltip } from '@/wax/components/tooltip'
import * as Table from '@/wax/components/table'
import { CELL_HEIGHT_PX, groupHeadingHeightPx } from '@/wax/components/table'
import {
  CatalogLayout,
  CatalogRail,
  CatalogRailItem,
  CatalogToolbar,
  toolbarActionLabel,
  type CatalogRailEmpty,
} from '@/components/catalog'
import { filterQuery, type FilterQuery } from '@/utils/filter-query'
import { EmptyPage } from '@/components/empty-page'
import {
  PolicyToggle,
  policyLabels,
  policyOptions,
  type PolicyState,
} from '@/components/policy-toggle'
import {
  byService,
  sourceSearch,
  type RecipeIcons,
  type ServiceChoices,
} from '@/lib/provider-recipes/choice'
import { ActionNotice, Fields } from '@/views/management/forms'
import { ArchivedBanner, trail } from '@/views/management/layout'

import { AddProviderLauncher, needsSourceDetails, type AddProviderEntryProps } from './add-provider'
import { countLabel } from '@/utils/count-label'
import { catalogMatches } from './provider-catalog'
import { PolicyMutations, usePolicyMutations } from './policy-mutations'
import { useReadRecipe } from './read-recipe'
import {
  encodeSelectedTool,
  parseSelectionKey,
  SELECTED_TOOL_FIELD,
  selectionKey,
  type SelectionKey,
} from './selection'
import type {
  FormContext,
  ToolHintsView,
  ToolView,
  ProviderView,
  WorkspaceProvidersPageProps,
} from './types'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import coralMarkDark from './coral-mark-dark.svg'
import coralMarkLight from './coral-mark-light.svg'
import * as styles from './providers.css'

/** Coral's own mark, which stands for the Built-in Provider. */
const CORAL_ICONS: RecipeIcons = { light: coralMarkLight, dark: coralMarkDark }

const bulkPolicyOptions = policyOptions.map(({ icon, ...option }) => ({
  ...option,
  icon: { color: 'secondary' as const, name: icon },
}))

// One label per declared hint. Coral never assumes a hint the provider did
// not declare, so a tool without hints shows no label.
const hintLabels: {
  hint: keyof ToolHintsView
  label: string
  declaredFalse: string
  color: PillColor
  tooltip: string
  falseTooltip: string
}[] = [
  {
    hint: 'readOnly',
    label: 'Read-only',
    declaredFalse: 'Not read-only',
    color: 'graySubtle',
    tooltip: 'The provider says this tool changes nothing.',
    falseTooltip: 'The provider says this tool can change data.',
  },
  {
    hint: 'destructive',
    label: 'Destructive',
    declaredFalse: 'Not destructive',
    color: 'red',
    tooltip: 'The provider says this tool can delete or overwrite data.',
    falseTooltip: 'The provider says this tool does not delete or overwrite data.',
  },
  {
    hint: 'idempotent',
    label: 'Idempotent',
    declaredFalse: 'Not idempotent',
    color: 'green',
    tooltip: 'The provider says a repeated call has no further effect.',
    falseTooltip: 'The provider says a repeated call can have a further effect.',
  },
  {
    hint: 'openWorld',
    label: 'Open world',
    declaredFalse: 'Not open world',
    color: 'graySubtle',
    tooltip: 'The provider says this tool reaches systems outside the provider.',
    falseTooltip: 'The provider says this tool stays inside the provider.',
  },
]

/**
 * The provider's declared hints as small labels. A row shows only the hints
 * declared true; the drawer also shows those declared false.
 */
function HintLabels({
  hints,
  declaredFalse = false,
}: {
  hints: ToolHintsView
  declaredFalse?: boolean
}) {
  return hintLabels.map(
    ({ hint, label, declaredFalse: falseLabel, color, tooltip, falseTooltip }) => {
      const value = hints[hint]
      if (value === undefined || (!value && !declaredFalse)) return null
      return (
        <Tooltip key={hint} content={value ? tooltip : falseTooltip} maxWidth={320}>
          <span className={styles.inlineTooltipTarget} tabIndex={0}>
            <Pill size="small" variant="outline" color={value ? color : 'graySubtle'}>
              {value ? label : falseLabel}
            </Pill>
          </span>
        </Tooltip>
      )
    },
  )
}

export function WorkspaceProvidersPage(props: WorkspaceProvidersPageProps) {
  return (
    <PolicyMutations>
      <WorkspaceProvidersContent {...props} />
    </PolicyMutations>
  )
}

function WorkspaceProvidersContent({
  workspace: w,
  providers: installed,
  recipes = [],
  selectedProvider: selectedProviderRoute,
  selectedTool,
  form,
  result,
  pending = false,
}: WorkspaceProvidersPageProps) {
  const owner = w.effectiveRole === 'owner'
  const active = w.state === 'active'
  const canChange = owner && active
  const providersPath = `${w.path}/providers`
  const newPath = `${providersPath}/new`
  const location = useLocation()
  const [railFilter, setRailFilter] = useState('')
  const sortedProviders = useMemo(() => sortProviders(installed), [installed])
  // A provider added from a recipe usually keeps the recipe's id as its
  // namespace, so that recipe's logo stands for it; otherwise a lettermark.
  // Coral's mark stands for the Built-in Provider.
  const recipeIcons = useMemo(
    () => new Map(recipes.flatMap((recipe) => (recipe.icons ? [[recipe.id, recipe.icons]] : []))),
    [recipes],
  )
  const providerIcons = useCallback(
    (provider: ProviderView) =>
      provider.builtin ? CORAL_ICONS : recipeIcons.get(provider.namespace),
    [recipeIcons],
  )
  const entry = {
    form,
    pending,
    newPath,
    recipes,
  }
  const notice = result
  const toolRoute = /\/providers\/([^/]+)(?:\/tools\/([^/?#]+))?\/?$/.exec(location.pathname)
  // The provider the URL names; none means every provider.
  const routedId = selectedProviderRoute ?? toolRoute?.[1] ?? null
  const selectedToolPath =
    selectedTool ?? (toolRoute?.[2] ? decodeURIComponent(toolRoute[2]) : null)
  const selected = sortedProviders.find((provider) => provider.id === routedId)
  const tool = selected?.tools.find((o) => o.relativePath === selectedToolPath)
  const empty = sortedProviders.length === 0
  const addButton = canChange ? <AddProviderLauncher {...entry} /> : null
  // The field echoes each keystroke at once; the rail follows with the
  // deferred value when React has time.
  const deferredRailFilter = useDeferredValue(railFilter)
  const filteredProviders = useMemo(() => {
    const query = filterQuery(deferredRailFilter)
    return sortedProviders.filter((i) => query.matches(providerLabel(i)))
  }, [sortedProviders, deferredRailFilter])
  const catalog = useMemo(() => byService(recipes), [recipes])
  const railEmpty =
    filteredProviders.length === 0
      ? railEmptyState({ query: deferredRailFilter, catalog, entry, canChange, owner })
      : null
  return (
    <PageLayout
      variant="split"
      breadcrumbs={trail([], 'Providers')}
      titled={!empty}
      compact
      header={
        <>
          {!active && (
            <ArchivedBanner workspace={w}>
              You cannot add or change providers until you restore the workspace.
            </ArchivedBanner>
          )}
          <ActionNotice result={notice} />
        </>
      }
    >
      {empty ? (
        <div className={styles.emptyProviders}>
          <EmptyPage
            iconName="Plug"
            title="No providers yet"
            description={
              canChange
                ? 'Add your first provider to start accessing data via Coral.'
                : owner
                  ? 'Restore the workspace before you add a provider.'
                  : 'Ask a workspace owner to add a provider.'
            }
            action={addButton}
          />
        </div>
      ) : (
        <ProvidersTableScreen
          titleInfo={
            !owner && (
              <InfoTip ariaLabel="About providers">
                As a member, you use the providers. Owners add, remove, and change them.
              </InfoTip>
            )
          }
          addButton={
            canChange && <AddProviderLauncher {...entry} labelClassName={toolbarActionLabel} />
          }
          railEmpty={railEmpty}
          canChange={canChange}
          filteredProviders={filteredProviders}
          form={form}
          providers={sortedProviders}
          providersPath={providersPath}
          providerIcons={providerIcons}
          railFilter={railFilter}
          selectedProvider={selected ?? null}
          selectedTool={tool ?? null}
          setRailFilter={setRailFilter}
        />
      )}
    </PageLayout>
  )
}

/**
 * A rail filter that matches no installed provider looks for it in the
 * catalog, and offers what the reader can add: the one service it found, the
 * catalog filtered to what it found, or a custom provider.
 */
function railEmptyState({
  query,
  catalog,
  entry,
  canChange,
  owner,
}: {
  query: string
  catalog: readonly ServiceChoices[]
  entry: Omit<AddProviderEntryProps, 'result' | 'onReading'>
  canChange: boolean
  owner: boolean
}): CatalogRailEmpty {
  const message = `No providers match “${query.trim()}”`
  if (!canChange)
    return {
      message,
      description: owner
        ? 'Restore the workspace before you add a provider.'
        : 'Ask a workspace owner to add it.',
    }
  const matches = catalogMatches(catalog, filterQuery(query))
  if (matches.length === 1)
    return {
      message,
      description: 'Add it from the catalog.',
      action: <CatalogMatch service={matches[0]} form={entry.form} newPath={entry.newPath} />,
    }
  if (matches.length > 1)
    return {
      message,
      description: `The catalog has ${matches.length} matches.`,
      action: (
        <AddProviderLauncher
          {...entry}
          variant="secondary"
          catalogQuery={query}
          label="Show matches"
          leading={<Button.Icon name="Search" />}
        />
      ),
    }
  return {
    message,
    description: 'It is not in the catalog.',
    action: (
      <AddProviderLauncher
        {...entry}
        variant="secondary"
        initialTab="custom"
        label="Add custom provider"
      />
    ),
  }
}

/**
 * The one catalog service a rail filter found: a button for each of its
 * sources, MCP before API, as the catalog orders them. A source that needs
 * details opens the add page on it; any other is read at once, as the catalog
 * reads it.
 */
function CatalogMatch({
  service: { shown, choices },
  form,
  newPath,
}: {
  service: ServiceChoices
  form: FormContext
  newPath: string
}) {
  const navigate = useNavigate()
  const { read, reading } = useReadRecipe(form, newPath)
  const sources = choices.toSorted((a, b) => Number(b.kind === 'mcp') - Number(a.kind === 'mcp'))
  return (
    <div className={styles.railSources}>
      {sources.map((choice) => {
        const label =
          sources.length > 1
            ? `Add ${shown.displayName} ${choice.kind === 'mcp' ? 'MCP' : 'API'}`
            : `Add ${shown.displayName}`
        const busy = reading?.id === choice.id
        return (
          <Button.Container
            key={choice.id}
            type="button"
            variant="secondary"
            size="32"
            hasPrefix
            disabled={Boolean(reading)}
            aria-busy={busy ? true : undefined}
            onClick={() =>
              needsSourceDetails(choice)
                ? navigate(`${newPath}?${sourceSearch(choice)}`)
                : read(choice)
            }
          >
            {busy ? (
              <Button.SpinningButtonIcon name="Loader" />
            ) : (
              <Logo name={shown.displayName} icons={shown.icons} size="18" />
            )}
            <Button.Text tooltip={label}>{label}</Button.Text>
          </Button.Container>
        )
      })}
    </div>
  )
}

function providerLabel(provider: ProviderView) {
  return provider.displayName || provider.namespace
}

// Labels sort without regard to case, so "api" and "Zebra" do not split by it.
function caseFolded(value: string) {
  return value.toLocaleLowerCase()
}

// Coral's Built-in Provider leads; the providers you add follow by label.
function sortProviders(providers: ProviderView[]) {
  return providers.toSorted(
    (a, b) =>
      Number(b.builtin) - Number(a.builtin) ||
      caseFolded(providerLabel(a)).localeCompare(caseFolded(providerLabel(b))),
  )
}

function sortTools(tools: ToolView[]) {
  return tools.toSorted((a, b) =>
    caseFolded(a.relativePath).localeCompare(caseFolded(b.relativePath)),
  )
}

type SearchableProvider = {
  provider: ProviderView
  sorted: ToolView[]
}

function searchableProviders(providers: ProviderView[]): SearchableProvider[] {
  return providers.map((provider) => ({
    provider,
    sorted: sortTools(provider.tools),
  }))
}

/**
 * A summary count the reader clicked: the tools it covered at that
 * moment. A later policy change does not move a row out of the filter, so the
 * row the reader just changed stays where it is until the filter changes.
 */
type PolicyFilter = { value: PolicyState | 'unavailable'; keys: ReadonlySet<SelectionKey> }

type FilterByPolicy = (
  value: PolicyFilter['value'],
  inFlight: ReturnType<typeof usePolicyMutations>,
) => void

function policyFilterLabel(value: PolicyFilter['value']) {
  return value === 'unavailable' ? 'Unavailable' : policyLabels[value]
}

function matchingTools(
  { provider, sorted }: SearchableProvider,
  tableFilter: FilterQuery,
  policyFilter: PolicyFilter | null,
  selectedView: ReadonlySet<SelectionKey> | null,
) {
  // The filter matches only the name the row shows, so every match is visible.
  return sorted.filter((tool) => {
    const key = selectionKey(provider.id, tool.name)
    return (
      tableFilter.matches(tool.relativePath) &&
      (!policyFilter || policyFilter.keys.has(key)) &&
      (!selectedView || selectedView.has(key))
    )
  })
}

/** The rows a provider lists: what matches the filter, plus the tool the
 * drawer shows, which stays in place however the filter reads. It is not a
 * match, so "select all matching" and the hidden count leave it out. */
function listedTools(
  { sorted }: SearchableProvider,
  matching: ToolView[],
  linkedTool: ToolView | null,
) {
  if (!linkedTool || matching.includes(linkedTool)) return matching
  return sorted.filter((o) => o === linkedTool || matching.includes(o))
}

// A row the keyboard cursor rests on.
type CursorStop =
  | { kind: 'heading'; key: string; index: number; depth: 0 | 1; collapsed: boolean }
  | {
      kind: 'tool'
      key: SelectionKey
      index: number
      provider: ProviderView
      tool: ToolView
    }

type TableRowItem =
  | { kind: 'provider'; key: string; provider: ProviderView }
  | { kind: 'group'; key: string; provider: ProviderView; group: string }
  | { kind: 'tool'; key: string; provider: ProviderView; tool: ToolView }
  | { kind: 'status'; key: string; provider: ProviderView; message: string }

// The same rows as a tree, so the table renders them as nested `Table.Group`s
// and Wax keeps the provider and tool-group headings sticky. `index` is a
// row's position in the flat list the virtualizer measures.
type ProviderSection = {
  provider: ProviderView
  index: number
  /** Its key in the collapsed set; collapsed, the section lists no rows. */
  key: string
  collapsed: boolean
  /** Every match, collapsed or not: the heading counts and selects these. */
  tools: ToolView[]
  status: string | null
  statusIndex: number | null
  /** The filter matched none of the provider's tools. */
  noMatch: boolean
  groups: {
    group: string
    index: number
    key: string
    collapsed: boolean
    /** Every listed member, collapsed or not: the heading counts and selects these. */
    members: ToolView[]
    /** Each listed row with its index in the flat row list, for the virtualizer. */
    tools: { index: number; tool: ToolView }[]
  }[]
}

// Every heading mounts, so Wax's sticky groups work as they do without a
// virtualizer; only the tool rows inside a group come and go. Each visible
// tool costs a policy form, which is what made the full table slow.
const DENSITY = 'comfortable'
// Provider and group headings are prominent: they stand taller than the tool rows.
const GROUP_HEADINGS = 'prominent'
const HEADING_HEIGHT = groupHeadingHeightPx(DENSITY, GROUP_HEADINGS)
// Until a row mounts and measures itself: a tool row is a cell's height
// plus its border; a provider's status row is as tall as an ordinary row.
const TOOL_ROW_HEIGHT = CELL_HEIGHT_PX[DENSITY] + 4
const STATUS_ROW_HEIGHT = CELL_HEIGHT_PX[DENSITY]

const tableRowHeight = (row: TableRowItem) => {
  switch (row.kind) {
    case 'provider':
    case 'group':
      return HEADING_HEIGHT
    case 'tool':
      return TOOL_ROW_HEIGHT
    case 'status':
      return STATUS_ROW_HEIGHT
  }
}

function ProvidersTableScreen({
  titleInfo,
  addButton,
  canChange,
  filteredProviders,
  form,
  providers,
  providersPath,
  providerIcons,
  railEmpty,
  railFilter,
  selectedProvider,
  selectedTool,
  setRailFilter,
}: {
  /** An info tip beside the title. */
  titleInfo: ReactNode
  /** The main action, at the end of the toolbar so it shows on every tab. */
  addButton: ReactNode
  canChange: boolean
  filteredProviders: ProviderView[]
  form: FormContext
  providers: ProviderView[]
  /** The address of every provider's tab. */
  providersPath: string
  /** Recipe logos by namespace, for providers that kept the recipe's id. */
  providerIcons: (provider: ProviderView) => RecipeIcons | undefined
  /** What the rail shows when its filter matches no provider; null while one matches. */
  railEmpty: CatalogRailEmpty | null
  railFilter: string
  /** The provider the URL names, if any. */
  selectedProvider: ProviderView | null
  selectedTool: ToolView | null
  setRailFilter: (value: string) => void
}) {
  const navigate = useNavigate()
  const mutations = usePolicyMutations()
  const [tableFilterInput, setTableFilterInput] = useState('')
  // The field echoes each keystroke at once; the table, which rebuilds
  // thousands of rows, follows with the deferred value when React has time.
  const tableFilter = useDeferredValue(tableFilterInput)
  const tableQuery = useMemo(() => filterQuery(tableFilter), [tableFilter])
  const [policyFilter, setPolicyFilter] = useState<PolicyFilter | null>(null)
  const [selected, setSelected] = useState<Set<SelectionKey>>(new Set())
  // "Show selected" lists the selection as it was when clicked, like a policy
  // filter, so a row the reader unchecks or applies a policy to stays in view.
  const [selectedView, setSelectedView] = useState<ReadonlySet<SelectionKey> | null>(null)
  // The provider and group headings the reader collapsed, by row key. A filter
  // leaves them collapsed; their headings still count the matches.
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set())
  const tableRef = useRef<HTMLDivElement>(null)
  const searchable = useMemo(() => searchableProviders(providers), [providers])
  const selectedProviderId = selectedProvider?.id ?? null
  const selectedToolName = selectedTool?.name ?? null
  // The rail is a set of tabs: the table shows one provider, or every one.
  // A provider route picks the tab. A tool route opens the drawer over
  // the tab the reader is on when that tab lists it, so the rows under it
  // stay; otherwise it opens its provider's tab. Selection is keyed, not
  // row-derived, so it survives a change of tab and counts as hidden there.
  const [lastTab, setLastTab] = useState(selectedProviderId)
  const keepsTab = selectedToolName && (lastTab === null || lastTab === selectedProviderId)
  const routeTab = keepsTab ? undefined : selectedProviderId
  if (routeTab !== undefined && routeTab !== lastTab) setLastTab(routeTab)
  const tab = routeTab === undefined ? lastTab : routeTab
  const visibleByProvider = useMemo(() => {
    const entries = searchable
      .filter((entry) => tab === null || entry.provider.id === tab)
      .map((entry) => {
        const matching = matchingTools(entry, tableQuery, policyFilter, selectedView)
        const linked = selectedProvider?.id === entry.provider.id ? selectedTool : null
        return {
          provider: entry.provider,
          matching,
          listed: listedTools(entry, matching, linked),
        }
      })
    // The selected view lists only the providers it covers.
    if (!selectedView) return entries
    return entries.filter(({ provider }) =>
      provider.tools.some((o) => selectedView.has(selectionKey(provider.id, o.name))),
    )
  }, [policyFilter, searchable, selectedTool, selectedProvider?.id, selectedView, tab, tableQuery])
  const visibleTools = visibleByProvider.flatMap(({ provider, matching }) =>
    matching.map((tool) => ({ provider, tool })),
  )
  const allVisibleSelected =
    visibleTools.length > 0 &&
    visibleTools.every(({ provider, tool }) => selected.has(selectionKey(provider.id, tool.name)))
  const visibleSelectedCount = visibleTools.filter(({ provider, tool }) =>
    selected.has(selectionKey(provider.id, tool.name)),
  ).length
  const { rows, sections } = useMemo(() => {
    const flat: TableRowItem[] = []
    const tree: ProviderSection[] = []
    for (const { provider, matching, listed } of visibleByProvider) {
      // The tool the drawer shows opens its provider and group, so its row is there.
      const linked = provider.id === selectedProviderId ? selectedTool : null
      const key = providerHeadingKey(provider.id)
      const section: ProviderSection = {
        provider,
        index: flat.length,
        key,
        collapsed: collapsed.has(key) && !(linked && listed.includes(linked)),
        tools: matching,
        status: null,
        statusIndex: null,
        noMatch: false,
        groups: [],
      }
      tree.push(section)
      flat.push({ kind: 'provider', key, provider })
      if (section.collapsed) continue
      if (provider.tools.length === 0) {
        section.status = 'No tools yet. Settings remain available.'
        section.statusIndex = flat.length
        flat.push({
          kind: 'status',
          key: `empty:${provider.id}`,
          provider,
          message: section.status,
        })
        continue
      }
      if ((tableFilter.trim() || policyFilter || selectedView) && listed.length === 0) {
        const query = tableFilter.trim()
        section.status = query ? `No tools match “${query}”.` : 'No matching tools.'
        section.noMatch = true
        section.statusIndex = flat.length
        flat.push({
          kind: 'status',
          key: `nomatch:${provider.id}`,
          provider,
          message: section.status,
        })
        continue
      }
      const groups = new Map<string, ToolView[]>()
      for (const tool of listed) {
        const label = groupLabel(tool)
        const members = groups.get(label)
        if (members) members.push(tool)
        else groups.set(label, [tool])
      }
      for (const [group, members] of [...groups.entries()].toSorted(([a], [b]) =>
        caseFolded(a).localeCompare(caseFolded(b)),
      )) {
        const groupKey = groupHeadingKey(provider.id, group)
        const groupSection: ProviderSection['groups'][number] = {
          group,
          index: flat.length,
          key: groupKey,
          collapsed: collapsed.has(groupKey) && !(linked && members.includes(linked)),
          members,
          tools: [],
        }
        section.groups.push(groupSection)
        flat.push({ kind: 'group', key: groupKey, provider, group })
        if (groupSection.collapsed) continue
        for (const tool of members) {
          groupSection.tools.push({ index: flat.length, tool })
          flat.push({
            kind: 'tool',
            key: `tool:${provider.id}:${tool.name}`,
            provider,
            tool,
          })
        }
      }
    }
    return { rows: flat, sections: tree }
  }, [
    collapsed,
    policyFilter,
    selectedTool,
    selectedProviderId,
    selectedView,
    tableFilter,
    visibleByProvider,
  ])
  const toolIndexes = useMemo(() => {
    const indexes = new Map<string, number>()
    rows.forEach((row, index) => {
      if (row.kind === 'tool') indexes.set(selectionKey(row.provider.id, row.tool.name), index)
    })
    return indexes
  }, [rows])
  const virtual = Table.useVirtualRows({
    density: DENSITY,
    count: rows.length,
    estimateSize: (index) => tableRowHeight(rows[index]!),
    scrollRef: tableRef,
  })
  // Collapsing a group the reader scrolled into would leave them further down
  // the table; its heading comes back to where it pins instead.
  const toggleCollapsed = useCallback(
    (key: string, index: number, depth: number) => {
      setCollapsed((current) => {
        const next = new Set(current)
        if (!next.delete(key)) next.add(key)
        return next
      })
      requestAnimationFrame(() => virtual.revealIndex(index, { depth }))
    },
    [virtual],
  )
  // The headings the view lists, groups inside collapsed providers included,
  // so collapsing all leaves each provider's groups collapsed when it opens.
  const headingKeys = useMemo(
    () =>
      visibleByProvider.flatMap(({ provider, listed }) => [
        providerHeadingKey(provider.id),
        ...new Set(listed.map((o) => groupHeadingKey(provider.id, groupLabel(o)))),
      ]),
    [visibleByProvider],
  )
  // A provider shows rows while it is open and lists one open group, or no groups.
  const anyExpanded = sections.some(
    (section) =>
      !section.collapsed &&
      (section.groups.length === 0 || section.groups.some((group) => !group.collapsed)),
  )
  const toggleAllCollapsed = () => {
    setCollapsed((current) => {
      const next = new Set(current)
      for (const key of headingKeys) {
        if (anyExpanded) next.add(key)
        else next.delete(key)
      }
      return next
    })
    tableRef.current?.scrollTo?.({ top: 0 })
  }
  // A new tab starts at its top. This runs before the reveal, so an
  // tool route still scrolls to its row.
  useEffect(() => {
    tableRef.current?.scrollTo?.({ top: 0 })
    // oxlint-disable-next-line react/exhaustive-effect-dependencies -- A change of tab, not anything the effect reads, owns this scroll.
  }, [tab])
  const revealIndex =
    selectedProviderId && selectedToolName
      ? toolIndexes.get(selectionKey(selectedProviderId, selectedToolName))
      : undefined
  const markRevealed = useRevealScroll(
    virtual,
    selectedToolName ? selectedProviderId : null,
    selectedToolName,
    revealIndex,
  )
  const tabPath = (id: string | null) =>
    providers.find((provider) => provider.id === id)?.path ?? providersPath

  // A click reads the policies the summary shows, changes in flight included.
  const policyFilterFor = useCallback(
    (value: PolicyFilter['value'], inFlight: ReturnType<typeof usePolicyMutations>) => {
      const keys = new Set<SelectionKey>()
      for (const provider of providers)
        for (const tool of provider.tools)
          if (
            value === 'unavailable'
              ? !tool.available
              : effectivePolicy(inFlight, provider, tool) === value
          )
            keys.add(selectionKey(provider.id, tool.name))
      return { value, keys }
    },
    [providers],
  )
  // A heading count toggles its filter; the toolbar menu sets one.
  // A summary count, like the menu, leaves the selected view.
  const filterByPolicy: FilterByPolicy = useCallback(
    (value, inFlight) => {
      setSelectedView(null)
      setPolicyFilter((current) =>
        current?.value === value ? null : policyFilterFor(value, inFlight),
      )
    },
    [policyFilterFor],
  )

  // The selected view clears the searches and opens every provider's tab, so
  // it shows the whole selection.
  const showSelected = useCallback(() => {
    setTableFilterInput('')
    setRailFilter('')
    setPolicyFilter(null)
    setSelectedView(new Set(selected))
    if (tab !== null) navigate(providersPath, { preventScrollReset: true })
  }, [navigate, providersPath, selected, setRailFilter, tab])

  const toggleTools = useCallback(
    (targets: { provider: ProviderView; tool: ToolView }[], checked: boolean) => {
      setSelected((current) => {
        const next = new Set(current)
        for (const { provider, tool } of targets) {
          const key = selectionKey(provider.id, tool.name)
          if (checked) next.add(key)
          else next.delete(key)
        }
        return next
      })
    },
    [],
  )
  // A tab's address names it, so the URL can be shared. Each tab pushes an
  // entry, so Back returns to the tab before it. The route change is a
  // selection within the page, which the parent loader does not revalidate.
  const openTab = (path: string) => navigate(path, { preventScrollReset: true })
  // Set when the keys move the cursor: its row takes focus once it renders, so
  // Enter and Space reach it wherever focus was.
  const focusCursor = useRef(false)
  const closeSelectedRoute = () => {
    // The cursor stays on the tool the drawer showed, and its row takes
    // focus, so the reader sees where they were and the keys go on from it.
    setCursor(drawerKey)
    setClosedKey(drawerKey)
    focusCursor.current = true
    openTab(tabPath(tab))
  }

  // The keyboard cursor: the heading or tool ↑ and ↓ reach. An open
  // drawer holds it.
  const [cursor, setCursor] = useState<string | null>(null)
  const routedKey =
    selectedProviderId && selectedToolName
      ? selectionKey(selectedProviderId, selectedToolName)
      : null
  // A drawer the reader closed while its route change is still pending. The
  // keys treat it as closed, so ↓ right after Escape does not open the next one.
  const [closedKey, setClosedKey] = useState<string | null>(null)
  if (closedKey && routedKey !== closedKey) setClosedKey(null)
  const drawerKey = routedKey === closedKey ? null : routedKey
  const activeKey = drawerKey ?? cursor
  // A press outside the cursor's row and the filter ends the cursor; the keys
  // start a new one.
  useEffect(() => {
    if (!cursor || drawerKey) return
    const clear = (event: PointerEvent) => {
      const keep = [
        tableRef.current?.querySelector('[data-active]'),
        document.getElementById(TOOL_FILTER_ID),
      ]
      if (event.target instanceof Node && keep.some((at) => at?.contains(event.target as Node)))
        return
      setCursor(null)
    }
    document.addEventListener('pointerdown', clear)
    return () => document.removeEventListener('pointerdown', clear)
  }, [cursor, drawerKey, setCursor])
  // Each heading and tool the table lists, in order: the stops ↑ and ↓ reach.
  const cursorOrder = useMemo(() => {
    const headings = new Map<string, boolean>()
    for (const section of sections) {
      headings.set(section.key, section.collapsed)
      for (const group of section.groups) headings.set(group.key, group.collapsed)
    }
    return rows.flatMap((row, index): CursorStop[] => {
      if (row.kind === 'tool') {
        const key = selectionKey(row.provider.id, row.tool.name)
        return [{ kind: 'tool', key, index, provider: row.provider, tool: row.tool }]
      }
      if (row.kind === 'status') return []
      const depth = row.kind === 'provider' ? 0 : 1
      return [{ kind: 'heading', key: row.key, index, depth, collapsed: !!headings.get(row.key) }]
    })
  }, [rows, sections])
  const activeStop = cursorOrder.find((stop) => stop.key === activeKey)
  useEffect(() => {
    if (!focusCursor.current) return
    const row = tableRef.current?.querySelector<HTMLElement>('[data-active]')
    if (!row) return
    focusCursor.current = false
    row.focus({ preventScroll: true })
  })
  const moveCursor =
    (step: 1 | -1, inDrawer = false) =>
    (event: KeyboardEvent) => {
      if (!cursorKeyApplies(event, { fromFilter: true, overControls: true, inDrawer })) return
      event.preventDefault()
      // The open drawer shows tools, so it steps over the headings.
      const order = drawerKey ? cursorOrder.filter((stop) => stop.kind === 'tool') : cursorOrder
      const at = activeStop ? order.indexOf(activeStop) : -1
      const next =
        at < 0
          ? step > 0
            ? order[0]
            : order.at(-1)
          : order[Math.min(Math.max(at + step, 0), order.length - 1)]
      if (!next || next === activeStop) return
      setCursor(next.key)
      // A row sits under its provider and group headings.
      virtual.revealIndex(next.index, { depth: next.kind === 'tool' ? 2 : next.depth })
      // Typing in the filter goes on; the drawer keeps its own focus.
      const fromFilter = event.target instanceof Element && event.target.id === TOOL_FILTER_ID
      if (!fromFilter && !drawerKey) focusCursor.current = true
      if (drawerKey && next.kind === 'tool') {
        markRevealed(next.provider.id, next.tool.name)
        navigate(next.tool.path, { replace: true, preventScrollReset: true })
      }
    }
  const toggleHeading = (stop: Extract<CursorStop, { kind: 'heading' }>) =>
    toggleCollapsed(stop.key, stop.index, stop.depth)
  // On a tool, ← and → step its policy through the toggle's order,
  // stopping at either end. On a heading, ← collapses it and → expands it.
  const stepPolicy =
    (step: 1 | -1, inDrawer = false) =>
    (event: KeyboardEvent) => {
      if (!activeStop) return
      if (!cursorKeyApplies(event, { fromFilter: false, overControls: false, inDrawer })) return
      if (activeStop.kind === 'heading') {
        if (drawerKey) return
        event.preventDefault()
        if (activeStop.collapsed === step > 0) toggleHeading(activeStop)
        return
      }
      if (!canChange) return
      event.preventDefault()
      const { provider, tool } = activeStop
      const mutation = mutations.state(provider.id, tool.name)
      if (mutation.pending) return
      const order = policyOptions.map((option) => option.value)
      const next = order[order.indexOf(mutation.policy ?? tool.policy) + step]
      if (next) mutations.submit(policyFormData(form, provider, tool, next), provider.path)
    }
  // Enter opens the cursor's tool, or collapses or expands its heading.
  const openCursor = (event: KeyboardEvent) => {
    if (drawerKey || !activeStop) return
    if (!cursorKeyApplies(event, { fromFilter: true, overControls: false })) return
    event.preventDefault()
    if (activeStop.kind === 'heading') return toggleHeading(activeStop)
    markRevealed(activeStop.provider.id, activeStop.tool.name)
    navigate(activeStop.tool.path, { preventScrollReset: true })
  }
  // Space checks the cursor's tool, or collapses or expands its heading.
  const checkCursor = (event: KeyboardEvent) => {
    if (drawerKey || !activeStop) return
    if (!cursorKeyApplies(event, { fromFilter: false, overControls: false })) return
    if (activeStop.kind === 'heading') {
      event.preventDefault()
      return toggleHeading(activeStop)
    }
    if (!canChange || mutations.bulkPending) return
    event.preventDefault()
    toggleTools([activeStop], !selected.has(activeStop.key))
  }

  return (
    <CatalogLayout
      rail={
        <CatalogRail
          label="installed providers"
          filter={{
            id: 'provider-filter',
            label: 'Filter providers',
            value: railFilter,
            onChange: setRailFilter,
          }}
          empty={railEmpty}
        >
          <CatalogRailItem
            to={providersPath}
            current={tab === null}
            onSelect={() => openTab(providersPath)}
            leading={<Icon name="Layers" size="20" color="secondary" />}
            name="All providers"
            count={providers.length}
          />
          {filteredProviders.map((provider) => (
            <CatalogRailItem
              key={provider.id}
              to={provider.path}
              current={provider.id === tab}
              onSelect={() => openTab(provider.path)}
              leading={
                <>
                  <StatusDot
                    blockers={provider.blockers}
                    personalAccountAction={provider.personalAccountAction}
                  />
                  <Logo name={providerLabel(provider)} icons={providerIcons(provider)} size="20" />
                </>
              }
              name={providerLabel(provider)}
              trailing={
                // Coral has no settings: nobody renames, reconfigures, or removes it.
                !provider.builtin && (
                  <Button.Container
                    as={Link}
                    id={providerSettingsDomId(provider.id)}
                    to={`${provider.path}/settings`}
                    preventScrollReset
                    variant="bare"
                    size="22"
                    ariaLabel={`${providerLabel(provider)} settings`}
                  >
                    <Button.Icon name="Settings" />
                  </Button.Container>
                )
              }
            />
          ))}
        </CatalogRail>
      }
    >
      <CatalogToolbar
        title="Providers"
        titleInfo={titleInfo}
        action={addButton}
        filter={{
          id: TOOL_FILTER_ID,
          label: 'Filter tools',
          shortcut: '$mod+f',
          value: tableFilterInput,
          onChange: setTableFilterInput,
        }}
      >
        <PolicyFilterMenu
          value={selectedView ? 'selected' : (policyFilter?.value ?? null)}
          selectedCount={selected.size}
          onChange={(value) => {
            if (value === 'selected') return showSelected()
            setSelectedView(null)
            setPolicyFilter(value ? policyFilterFor(value, mutations) : null)
          }}
        />
      </CatalogToolbar>
      {!canChange && (
        <Banner variant="info">Only owners can change policy in an active workspace.</Banner>
      )}
      <ActionBar.Frame>
        <Table.Container
          ariaLabel="Tools"
          density={DENSITY}
          groupHeadings={GROUP_HEADINGS}
          layout="scroll"
          ref={tableRef}
          className={styles.providersTable}
          columns={[{ width: 44 }, { width: 'minmax(340px, 1fr)' }, { width: 'max-content' }]}
        >
          <Table.Head>
            <Table.Row>
              <span role="columnheader" aria-label="Select" className={Table.selectionCell}>
                <Inputs.Checkbox
                  fill
                  checked={allVisibleSelected}
                  indeterminate={visibleSelectedCount > 0 && !allVisibleSelected}
                  disabled={!canChange || mutations.bulkPending || visibleTools.length === 0}
                  onCheckedChange={(checked) => toggleTools(visibleTools, checked)}
                >
                  <span className={visuallyHidden}>Select all matching tools</span>
                </Inputs.Checkbox>
              </span>
              <span
                role="columnheader"
                className={classNames(styles.nameHeader, Table.groupToggleHeading)}
              >
                <Button.IconButton
                  variant="bare"
                  size="22"
                  name={anyExpanded ? 'ChevronsDownUp' : 'ChevronsUpDown'}
                  tooltipText={anyExpanded ? 'Collapse all' : 'Expand all'}
                  disabled={sections.length === 0}
                  onClick={toggleAllCollapsed}
                />
                Tool name
              </span>
              <span role="columnheader" className={styles.policyHeader}>
                Policy
              </span>
            </Table.Row>
          </Table.Head>
          {sections.length === 0 && <Table.Status>No providers match.</Table.Status>}
          <TableSections
            activeKey={activeKey}
            bulkPending={mutations.bulkPending}
            canChange={canChange}
            onToggleCollapsed={toggleCollapsed}
            drawerOpen={drawerKey !== null}
            filterByPolicy={filterByPolicy}
            markRevealed={markRevealed}
            form={form}
            policyFilter={policyFilter?.value ?? null}
            query={tableQuery}
            virtual={virtual}
            providerIcons={providerIcons}
            sections={sections}
            selected={selected}
            showSelected={showSelected}
            toggleTools={toggleTools}
          />
        </Table.Container>
        {selected.size > 0 && (
          <ActionBar.Bar>
            <BulkPolicyForm
              canChange={canChange}
              form={form}
              providers={providers}
              selected={selected}
              showingSelected={selectedView !== null}
              onShowSelected={() => (selectedView ? setSelectedView(null) : showSelected())}
              onClear={() => {
                setSelected(new Set())
                setSelectedView(null)
              }}
              onConfirmed={(confirmed) =>
                setSelected((current) => {
                  const next = new Set(current)
                  for (const key of confirmed) next.delete(key)
                  return next
                })
              }
            />
          </ActionBar.Bar>
        )}
      </ActionBar.Frame>
      {/* `cursorKeyApplies` decides which fields keep a key, so fields don't skip it here. */}
      <KeyboardShortcut shortcut="ArrowDown" handler={moveCursor(1)} ignore={never} />
      <KeyboardShortcut shortcut="ArrowUp" handler={moveCursor(-1)} ignore={never} />
      <KeyboardShortcut shortcut="ArrowRight" handler={stepPolicy(1)} ignore={repeated} />
      <KeyboardShortcut shortcut="ArrowLeft" handler={stepPolicy(-1)} ignore={repeated} />
      <KeyboardShortcut shortcut="Enter" handler={openCursor} ignore={repeated} />
      <KeyboardShortcut shortcut="Space" handler={checkCursor} ignore={repeated} />
      <ToolDetailsDrawer
        tool={selectedTool}
        provider={selectedProvider ?? undefined}
        canChange={canChange}
        form={form}
        onClose={closeSelectedRoute}
        // The dialog keeps arrow keys inside it, so it moves the cursor itself.
        onKeyDown={(event) => {
          if (event.key === 'ArrowDown') moveCursor(1, true)(event)
          if (event.key === 'ArrowUp') moveCursor(-1, true)(event)
          if (event.key === 'ArrowRight' && !event.repeat) stepPolicy(1, true)(event)
          if (event.key === 'ArrowLeft' && !event.repeat) stepPolicy(-1, true)(event)
        }}
      />
    </CatalogLayout>
  )
}

// The table body is what a keystroke, a scroll, or a selection re-renders,
// and each heading and row is a tree of controls. Memoized on values, it only
// rebuilds when the deferred filter, the virtual window, or a selection lands;
// the urgent render that echoes a keystroke into the field skips it.
/**
 * Arriving on a provider or tool route reveals its row. Only the route's
 * target triggers this: filtering, which moves the row, must not re-scroll,
 * and closing a drawer, which narrows the target to its provider
 * or to none, must leave the table where the reader left it. Returns a
 * function for tools the reader opened in place, so their address does
 * not scroll again.
 */
function useRevealScroll(
  virtual: Table.VirtualRowsHandle,
  provider: string | null,
  tool: string | null,
  index: number | undefined,
) {
  const indexRef = useRef(index)
  indexRef.current = index
  const revealed = useRef<{ provider: string | null; tool: string | null }>({
    provider: null,
    tool: null,
  })
  useEffect(() => {
    const previous = revealed.current
    revealed.current = { provider, tool }
    const unchanged =
      provider === null ||
      (previous.provider === provider && (previous.tool === tool || tool === null))
    if (unchanged) return
    const target = indexRef.current
    // A tool sits under its provider and group headings; a provider under the head.
    if (target !== undefined) virtual.scrollToIndex(target, { depth: tool ? 2 : 0 })
    // oxlint-disable-next-line react-hooks/exhaustive-deps -- The selected route, not the virtual handle, owns this scroll.
  }, [provider, tool])
  return useCallback((providerId: string, toolName: string | null = null) => {
    revealed.current = { provider: providerId, tool: toolName }
  }, [])
}

const TableSections = memo(function TableSections({
  activeKey,
  bulkPending,
  canChange,
  drawerOpen,
  filterByPolicy,
  form,
  markRevealed,
  onToggleCollapsed,
  policyFilter,
  providerIcons,
  query,
  sections,
  selected,
  showSelected,
  toggleTools,
  virtual,
}: {
  /** The keyboard cursor's tool, or the drawer's while one is open. */
  activeKey: string | null
  bulkPending: boolean
  canChange: boolean
  drawerOpen: boolean
  filterByPolicy: FilterByPolicy
  form: FormContext
  /** Records a tool the reader opened in place, so its route does not scroll the table. */
  markRevealed: (provider: string, tool: string) => void
  /** Collapses or expands the heading with this key, at this row index and depth. */
  onToggleCollapsed: (key: string, index: number, depth: number) => void
  policyFilter: PolicyFilter['value'] | null
  providerIcons: (provider: ProviderView) => RecipeIcons | undefined
  /** The tool filter, bolded where it matches a name. */
  query: FilterQuery
  sections: ProviderSection[]
  selected: Set<SelectionKey>
  showSelected: () => void
  toggleTools: (targets: { provider: ProviderView; tool: ToolView }[], checked: boolean) => void
  virtual: Table.VirtualRowsHandle
}) {
  return sections.map((section) => (
    <Table.Group
      key={section.provider.id}
      id={providerGroupDomId(section.provider.id)}
      index={section.index}
      active={activeKey === section.key}
      label={providerLabel(section.provider)}
      collapsed={section.collapsed}
      heading={
        <GroupHeading
          canChange={canChange}
          bulkPending={bulkPending}
          className={styles.providerGroupLabel}
          collapsed={section.collapsed}
          onToggle={() => onToggleCollapsed(section.key, section.index, 0)}
          count={section.tools.length}
          total={section.provider.totalCount}
          label={
            <>
              <Logo
                name={providerLabel(section.provider)}
                icons={providerIcons(section.provider)}
                size="20"
              />
              <Typography.BodyStrong as="span">
                {providerLabel(section.provider)}
              </Typography.BodyStrong>
            </>
          }
          members={section.tools}
          name={providerLabel(section.provider)}
          provider={section.provider}
          summary={
            <PolicySummary
              active={policyFilter}
              onFilter={filterByPolicy}
              provider={section.provider}
            />
          }
          selected={selected}
          toggleTools={toggleTools}
        />
      }
    >
      {section.status && (
        <Table.Status
          scope="group"
          ref={virtual.measureRow}
          data-index={section.statusIndex ?? undefined}
        >
          {section.noMatch ? (
            <NoMatchStatus section={section} selected={selected} onShow={showSelected} />
          ) : (
            section.status
          )}
        </Table.Status>
      )}
      {section.groups.map((group) => (
        <Table.Group
          key={group.group}
          index={group.index}
          active={activeKey === group.key}
          label={group.group}
          collapsed={group.collapsed}
          heading={
            <GroupHeading
              bulkPending={bulkPending}
              canChange={canChange}
              className={styles.toolGroupLabel}
              collapsed={group.collapsed}
              onToggle={() => onToggleCollapsed(group.key, group.index, 1)}
              count={group.members.length}
              label={
                <Typography.BodySmallStrong as="span">{group.group}</Typography.BodySmallStrong>
              }
              members={group.members}
              name={group.group}
              provider={section.provider}
              selected={selected}
              toggleTools={toggleTools}
            />
          }
        >
          <Table.VirtualRows rows={group.tools} virtual={virtual}>
            {({ index, tool }) => (
              <ToolRow
                key={tool.name}
                active={activeKey === selectionKey(section.provider.id, tool.name)}
                drawerOpen={drawerOpen}
                canChange={canChange}
                checked={selected.has(selectionKey(section.provider.id, tool.name))}
                disabled={bulkPending}
                form={form}
                index={index}
                provider={section.provider}
                measure={virtual.measureRow}
                tool={tool}
                query={query}
                onOpen={markRevealed}
                onCheck={(checked) => toggleTools([{ provider: section.provider, tool }], checked)}
              />
            )}
          </Table.VirtualRows>
        </Table.Group>
      ))}
    </Table.Group>
  ))
})

const TOOL_FILTER_ID = 'tool-filter'

// A heading's key in the collapsed set and the cursor order.
const providerHeadingKey = (providerId: string) => `provider:${providerId}`
const groupHeadingKey = (providerId: string, group: string) => `group:${providerId}:${group}`
/** The tool group a heading lists the tool under. */
const groupLabel = (tool: ToolView) => tool.group || 'Other'
const PROVIDER_GROUP_DOM_ID = 'provider-group-'

function providerGroupDomId(provider: string) {
  return `${PROVIDER_GROUP_DOM_ID}${provider}`
}

function providerSettingsDomId(provider: string) {
  return `provider-settings-${provider}`
}

type ToggleTools = (targets: { provider: ProviderView; tool: ToolView }[], checked: boolean) => void

// The cells of a `Table.Group` heading, provider or tool group: a
// checkbox over the group's matching tools, then a label and count.
function GroupHeading({
  bulkPending,
  canChange,
  collapsed,
  onToggle,
  count,
  total = count,
  label,
  name,
  members,
  provider,
  selected,
  toggleTools,
  summary,
  ...cell
}: {
  bulkPending: boolean
  canChange: boolean
  collapsed: boolean
  onToggle: () => void
  /** The tools the heading counts; the checkbox covers `members`. */
  count: number
  /** Every tool under the heading; while a filter hides some, the heading shows both. */
  total?: number
  label: ReactNode
  /** The accessible name of the checkbox's target. */
  name: string
  members: ToolView[]
  provider: ProviderView
  selected: Set<SelectionKey>
  toggleTools: ToggleTools
  /** Closes the label cell, at the heading's end. */
  summary?: ReactNode
  className: string
}) {
  const targets = members.map((tool) => ({ provider, tool }))
  const selectedCount = members.filter((tool) =>
    selected.has(selectionKey(provider.id, tool.name)),
  ).length
  const allSelected = members.length > 0 && selectedCount === members.length
  return (
    <>
      <span role="cell" className={Table.selectionCell}>
        <Inputs.Checkbox
          fill
          checked={allSelected}
          indeterminate={selectedCount > 0 && !allSelected}
          disabled={!canChange || bulkPending || members.length === 0}
          onCheckedChange={(checked) => toggleTools(targets, checked)}
        >
          <span className={visuallyHidden}>{`Select ${name} tools`}</span>
        </Inputs.Checkbox>
      </span>
      <span role="cell" className={cell.className}>
        <Table.GroupToggle collapsed={collapsed} label={name} onToggle={onToggle} />
        {label}
        <Typography.BodySmall as="span" variant="tertiary">
          {count === total
            ? countLabel(count, 'tool')
            : `${count.toLocaleString('en-US')} of ${countLabel(total, 'tool')}`}
        </Typography.BodySmall>
        {summary && <span className={styles.policySummary}>{summary}</span>}
      </span>
    </>
  )
}

function BulkPolicyForm({
  canChange,
  form,
  onClear,
  onConfirmed,
  onShowSelected,
  providers,
  selected,
  showingSelected,
}: {
  canChange: boolean
  form: FormContext
  onClear: () => void
  onConfirmed: (confirmed: SelectionKey[]) => void
  /** Opens the selected view, or leaves it while it shows. */
  onShowSelected: () => void
  providers: ProviderView[]
  selected: Set<SelectionKey>
  showingSelected: boolean
}) {
  const mutations = usePolicyMutations()
  const [access, setAccess] = useState<PolicyState>('approval_required')
  // A selected tool with a change in flight (its own row's or a bulk
  // one) holds the form: a second write could target the same tool.
  const saving = [...selected].some((key) => {
    const { provider, tool } = parseSelectionKey(key)
    return mutations.state(provider, tool).pending
  })
  const disabled = !canChange || selected.size === 0 || saving
  // Every provider route serves the same policy action; the selection itself
  // names each tool's provider, so the first route stands for all.
  const anyProviderPath = providers[0]?.path ?? '#'
  return (
    <form
      method="post"
      action={anyProviderPath}
      aria-label="Bulk policy"
      className={styles.bulkPolicyForm}
      onSubmit={(event) => {
        event.preventDefault()
        if (disabled) return
        // The selection joins the form data here rather than as one hidden
        // input per tool: a whole-catalog selection is thousands of
        // inputs to mount and lay out on every selection change.
        const data = new FormData(event.currentTarget)
        for (const key of selected) {
          data.append(SELECTED_TOOL_FIELD, encodeSelectedTool(parseSelectionKey(key)))
        }
        mutations.submit(data, anyProviderPath, onConfirmed)
      }}
    >
      <Fields form={form} intent="policy" />
      <input type="hidden" name="access" value={access} />
      <Tooltip content={showingSelected ? 'Show all tools.' : 'Show only the selected tools.'}>
        <Button.TextButton
          type="button"
          variant="secondary"
          aria-pressed={showingSelected}
          onClick={onShowSelected}
        >
          {`${selected.size.toLocaleString('en-US')} selected`}
        </Button.TextButton>
      </Tooltip>
      <Picker
        label="Bulk policy"
        options={bulkPolicyOptions}
        value={access}
        onChange={(value) => setAccess(value as PolicyState)}
      />
      <Tooltip
        content={
          saving
            ? 'Wait for the selected policy changes to finish.'
            : 'Apply this policy to the selected tools.'
        }
      >
        <span tabIndex={saving ? 0 : undefined}>
          <Button.TextButton type="submit" disabled={disabled}>
            Apply
          </Button.TextButton>
        </span>
      </Tooltip>
      <Button.TextButton type="button" variant="bare" onClick={onClear} disabled={saving}>
        Clear selection
      </Button.TextButton>
    </form>
  )
}

const ToolRow = memo(function ToolRow({
  active,
  drawerOpen,
  canChange,
  checked,
  disabled,
  form,
  index,
  provider,
  measure,
  tool,
  query,
  onCheck,
  onOpen,
}: {
  /** The keyboard cursor rests here, or the drawer shows this tool. */
  active: boolean
  drawerOpen: boolean
  canChange: boolean
  checked: boolean
  disabled: boolean
  form: FormContext
  /** The row's place in the flat list the virtualizer measures. */
  index: number
  provider: ProviderView
  measure: Ref<HTMLDivElement>
  tool: ToolView
  query: FilterQuery
  onCheck: (checked: boolean) => void
  /** Runs before a click opens the tool, which the reader can already see. */
  onOpen: (provider: string, tool: string) => void
}) {
  const unavailableReason = tool.reasons.join(' ') || 'Coral did not provide a reason.'
  const { pending } = usePolicyMutations().state(provider.id, tool.name)
  const navigate = useNavigate()
  // A click anywhere but a control opens the tool's drawer. The name's
  // link navigates itself, so the row leaves its clicks alone.
  const open = (event: MouseEvent<HTMLDivElement>) => {
    const control = (event.target as Element).closest(ROW_CONTROLS + ', a')
    // The cursor row takes focus, so its own tabindex is not a control in it.
    if (control && control !== event.currentTarget && event.currentTarget.contains(control)) return
    onOpen(provider.id, tool.name)
    navigate(tool.path, { preventScrollReset: true })
  }
  return (
    <Table.Row
      className={classNames(pending && animations.pulseAnimation, checked && styles.checkedRow)}
      aria-busy={pending}
      data-index={index}
      ref={measure}
      onClick={open}
      selected={active && drawerOpen}
      active={active && !drawerOpen}
    >
      <span role="cell" className={Table.selectionCell}>
        <Inputs.Checkbox
          fill
          checked={checked}
          disabled={!canChange || disabled}
          onCheckedChange={onCheck}
        >
          <span className={visuallyHidden}>{`Select ${tool.relativePath}`}</span>
        </Inputs.Checkbox>
      </span>
      <span role="cell" className={styles.toolNameCell}>
        {/* The row opens the drawer; the name stays a link for the keyboard. */}
        <Tooltip content={tool.relativePath} showOnlyWhenTruncated>
          <Link
            to={tool.path}
            prefetch="intent"
            preventScrollReset
            onClick={() => onOpen(provider.id, tool.name)}
            className={styles.toolName}
          >
            <HighlightedMatches text={tool.relativePath} query={query} />
          </Link>
        </Tooltip>
        <HintLabels hints={tool.hints} />
        {!tool.available && (
          <Tooltip content={unavailableReason} maxWidth={420}>
            <span className={styles.inlineTooltipTarget} tabIndex={0}>
              <Pill size="small" color="amber">
                Unavailable
              </Pill>
            </span>
          </Tooltip>
        )}
      </span>
      <span role="cell" className={styles.policyCell}>
        <PolicySelect canChange={canChange} compact form={form} provider={provider} tool={tool} />
      </span>
    </Table.Row>
  )
})

// Clicks on these keep their own behavior and do not open the drawer.
const ROW_CONTROLS = 'button, input, label, [role="radio"], [role="checkbox"], [tabindex]'

// Controls that read these keys themselves: a radio group or menu moves with
// the arrows, and a field types the space.
const KEY_CONTROLS =
  'textarea, select, [contenteditable="true"], [role="radiogroup"], [role="menu"], [role="listbox"], [role="combobox"], [role="slider"], [role="tablist"], input'

const never = () => false
const repeated = (event: KeyboardEvent) => event.repeat

/** The fields a row's policy form posts, for a change made from the keyboard. */
function policyFormData(
  form: FormContext,
  provider: ProviderView,
  tool: ToolView,
  access: PolicyState,
) {
  const data = new FormData()
  data.set('csrf', form.csrf)
  data.set('requestId', form.requestId)
  data.set('intent', 'policy')
  data.set('provider', provider.id)
  data.set(SELECTED_TOOL_FIELD, encodeSelectedTool({ provider: provider.id, tool: tool.name }))
  data.set('access', access)
  return data
}

/**
 * Whether a cursor key belongs to the tools table. A control that reads
 * the key keeps it; so does any dialog but the tool drawer, which passes
 * `inDrawer`. With `fromFilter`, the key also works from the tool filter.
 * With `overControls`, it also works from a focused button, link, or checkbox,
 * which otherwise keep the Enter and Space that activate them.
 */
function cursorKeyApplies(
  event: KeyboardEvent,
  {
    fromFilter,
    overControls,
    inDrawer = false,
  }: { fromFilter: boolean; overControls: boolean; inDrawer?: boolean },
) {
  if (event.defaultPrevented || event.isComposing) return false
  const target = event.target instanceof Element ? event.target : null
  // The table scrolls, so it takes focus, and the cursor's row takes it from
  // the keys. Neither is a control in a row.
  if (!target || target === document.body || target.matches('[role="table"], [role="row"]'))
    return true
  if (target.id === TOOL_FILTER_ID) return fromFilter
  if (!overControls && target.closest(ROW_CONTROLS + ', a')) return false
  if (target.closest(KEY_CONTROLS)) return false
  return inDrawer || !target.closest('[role="dialog"], [role="alertdialog"]')
}

const PolicySelect = memo(function PolicySelect({
  canChange,
  compact = false,
  form,
  provider,
  tool,
}: {
  canChange: boolean
  /** Sized for a compact table row. */
  compact?: boolean
  form: FormContext
  provider: ProviderView
  tool: ToolView
}) {
  const mutations = usePolicyMutations()
  const mutation = mutations.state(provider.id, tool.name)
  const policy = mutation.policy ?? tool.policy
  const ref = useRef<HTMLFormElement>(null)
  return (
    <form
      method="post"
      action={provider.path}
      ref={ref}
      className={styles.policyCell}
      aria-busy={mutation.pending}
    >
      <Fields form={form} intent="policy" />
      <input type="hidden" name="provider" value={provider.id} />
      <input
        type="hidden"
        name={SELECTED_TOOL_FIELD}
        value={encodeSelectedTool({ provider: provider.id, tool: tool.name })}
      />
      <PolicyToggle
        size={compact ? 'small' : undefined}
        disabled={!canChange || mutation.pending}
        label="Policy"
        value={policy}
        onChange={(value) => {
          const data = new FormData(ref.current!)
          data.set('access', value)
          mutations.submit(data, provider.path)
        }}
      />
      {mutation.failure && (
        <Tooltip content={mutation.failure.reason} maxWidth={420}>
          <span className={styles.inlineTooltipTarget} tabIndex={0}>
            <Pill color="red">
              {mutation.failure.status === 'failed' ? 'Not saved' : 'Not confirmed'}
            </Pill>
          </span>
        </Tooltip>
      )}
    </form>
  )
})

const policyFilterOptions: { value: PolicyFilter['value']; label: string; icon: IconName }[] = [
  ...policyOptions.map(({ value, label, icon }) => ({ value, label, icon })),
  { value: 'unavailable', label: policyFilterLabel('unavailable'), icon: 'CircleAlert' },
]

/** A provider's no-match status. It counts the selected tools the filter
 * hides there and offers to show them, and says nothing more while none are.
 * It shows only when the provider lists no rows, so each of them is hidden. */
function NoMatchStatus({
  onShow,
  section,
  selected,
}: {
  onShow: () => void
  section: ProviderSection
  selected: Set<SelectionKey>
}) {
  const { provider } = section
  const hidden = provider.tools.filter((tool) =>
    selected.has(selectionKey(provider.id, tool.name)),
  ).length
  if (hidden === 0) return section.status
  // The copy stays one text run, so its sentences keep their space; the gap
  // only sets the button apart.
  return (
    <span className={styles.sectionStatus}>
      <span>
        {`${section.status} ${hidden} selected ${hidden === 1 ? 'tool is' : 'tools are'} hidden.`}
      </span>
      <Button.Container type="button" variant="secondary" size="22" onClick={onShow}>
        <Button.Icon name="ListFilter" />
        <Button.Text>Show all selected tools</Button.Text>
      </Button.Container>
    </span>
  )
}

type TableFilterValue = PolicyFilter['value'] | 'selected'

/** Narrows the table to the tools one policy covers. It sits beside the search. */
function PolicyFilterMenu({
  value,
  selectedCount,
  onChange,
}: {
  value: TableFilterValue | null
  selectedCount: number
  onChange: (value: TableFilterValue | null) => void
}) {
  const active =
    value === 'selected'
      ? { icon: 'ListChecks' as const, label: 'Selected' }
      : policyFilterOptions.find((option) => option.value === value)
  return (
    <Menu.Container>
      <Menu.Trigger render={<Button.Container variant="secondary" ariaLabel="Filter tools by" />}>
        <Button.Icon name={active?.icon ?? 'ListFilter'} />
        <Button.Text>{active?.label ?? 'All policies'}</Button.Text>
        <Button.Icon name="ChevronDown" />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.RadioGroup
          value={value ?? 'all'}
          onValueChange={(next: string) =>
            onChange(next === 'all' ? null : (next as TableFilterValue))
          }
        >
          <Menu.RadioItem value="all" iconName="ListFilter">
            All policies
          </Menu.RadioItem>
          {policyFilterOptions.map((option) => (
            <Menu.RadioItem key={option.value} value={option.value} iconName={option.icon}>
              {option.label}
            </Menu.RadioItem>
          ))}
          <Menu.Separator />
          <Menu.RadioItem
            value="selected"
            iconName="ListChecks"
            disabled={selectedCount === 0 && value !== 'selected'}
          >
            {`Selected (${selectedCount})`}
          </Menu.RadioItem>
        </Menu.RadioGroup>
      </Menu.Content>
    </Menu.Container>
  )
}

function effectivePolicy(
  mutations: ReturnType<typeof usePolicyMutations>,
  provider: ProviderView,
  tool: ToolView,
) {
  return mutations.state(provider.id, tool.name).policy ?? tool.policy
}

/**
 * How many of a provider's tools each policy covers, and how many are
 * unavailable, with the policy changes still in flight. Each count pairs its
 * icon with text, so the summary does not depend on color. A count filters
 * the table to its tools across every provider.
 */
function PolicySummary({
  active,
  onFilter,
  provider,
}: {
  active: PolicyFilter['value'] | null
  onFilter: FilterByPolicy
  provider: ProviderView
}) {
  const mutations = usePolicyMutations()
  const counts = new Map<PolicyState, number>()
  for (const tool of provider.tools) {
    const policy = effectivePolicy(mutations, provider, tool)
    counts.set(policy, (counts.get(policy) ?? 0) + 1)
  }
  const unavailable = provider.totalCount - provider.readyCount
  return (
    <>
      {provider.searchIndex?.state === 'pending' && (
        <Tooltip content="Search cannot find these tools yet. Agents can still call them.">
          <span className={styles.inlineTooltipTarget} tabIndex={0}>
            <Pill size="small" color="gray">
              <Icon name="Hourglass" size="14" color="inherit" />
              Indexing…
            </Pill>
          </span>
        </Tooltip>
      )}
      {provider.searchIndex?.state === 'failed' && (
        <Tooltip
          content={`Search cannot find these tools. Agents can still call them. Reason: ${provider.searchIndex.reason}`}
        >
          <span className={styles.inlineTooltipTarget} tabIndex={0}>
            <Pill size="small" color="amber">
              <Icon name="SearchX" size="14" color="inherit" />
              Not in search
            </Pill>
          </span>
        </Tooltip>
      )}
      {unavailable > 0 && (
        <Tooltip content="Show only unavailable tools.">
          <Button.Container
            variant="bare"
            size="22"
            className={styles.policySummaryItem}
            aria-pressed={active === 'unavailable'}
            onClick={() => onFilter('unavailable', mutations)}
          >
            <Pill size="small" color="amber">
              {`${unavailable} unavailable`}
            </Pill>
          </Button.Container>
        </Tooltip>
      )}
      {policyOptions.map(({ value, label, icon }) => (
        <Tooltip key={value} content={`Show only tools set to ${label}.`}>
          <Button.Container
            variant="bare"
            size="22"
            className={styles.policySummaryItem}
            aria-pressed={active === value}
            onClick={() => onFilter(value, mutations)}
          >
            <Icon name={icon} size="14" color="inherit" />
            <span aria-hidden>{counts.get(value) ?? 0}</span>
            <span className={visuallyHidden}>{`${counts.get(value) ?? 0} ${label}`}</span>
          </Button.Container>
        </Tooltip>
      ))}
    </>
  )
}

function ToolDetailsDrawer({
  provider: routedProvider,
  canChange,
  form,
  tool: routed,
  onClose,
  onKeyDown,
}: {
  provider: ProviderView | undefined
  canChange: boolean
  form: FormContext
  tool: ToolView | null
  /** Runs as the close starts, so the URL and the keys follow it at once. */
  onClose: () => void
  onKeyDown: (event: KeyboardEvent) => void
}) {
  // The drawer lives on the page, so the URL can close it at once. Its exit
  // then plays with the tool it last showed.
  const [shown, setShown] = useState({ tool: routed, provider: routedProvider })
  if (routed && (routed !== shown.tool || routedProvider !== shown.provider)) {
    setShown({ tool: routed, provider: routedProvider })
  }
  const { tool, provider } = shown
  // The drawer stays open from one tool to the next, so only its content
  // changes. The content mounts once per tool, which starts it at the top.
  const popup = useRef<HTMLDivElement>(null)
  const scrollToTop = useCallback((content: HTMLDivElement | null) => {
    if (content) popup.current?.scrollTo?.({ top: 0 })
  }, [])
  return (
    <Dialog.Root
      open={routed !== null}
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
    >
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup
          ref={popup}
          size="m"
          placement="right"
          onKeyDown={(event) => onKeyDown(event.nativeEvent)}
        >
          <Dialog.Title>{tool?.relativePath ?? 'Tool details'}</Dialog.Title>
          {tool && (
            <div key={tool.name} ref={scrollToTop} className={styles.stack}>
              {tool.description.trim() && tool.description.trim() !== tool.summary.trim() ? (
                <Markdown>{tool.description}</Markdown>
              ) : (
                <Typography.BodySmall as="p">
                  {tool.summary || 'No description.'}
                </Typography.BodySmall>
              )}
              <div className={styles.callPath}>
                <CodeChip>{tool.callPath}</CodeChip>
                <Button.CopyButton
                  textToCopy={tool.callPath}
                  ariaLabel="Copy call path"
                  size="22"
                  variant="bare"
                />
              </div>
              <div className={styles.pills}>
                <HintLabels hints={tool.hints} declaredFalse />
                {provider && (
                  <PolicySelect canChange={canChange} form={form} provider={provider} tool={tool} />
                )}
              </div>
              {provider?.searchIndex?.state === 'pending' && (
                <Banner variant="info" title="Indexing…">
                  Search cannot find this tool yet. Agents can still call it.
                </Banner>
              )}
              {provider?.searchIndex?.state === 'failed' && (
                <Banner variant="info" title="Not in search">
                  Search cannot find this tool. Agents can still call it.
                </Banner>
              )}
              {!tool.available && (
                <Banner variant="warning" title="Unavailable">
                  {tool.reasons.join(' ') || 'Coral did not provide a reason.'}
                </Banner>
              )}
            </div>
          )}
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
