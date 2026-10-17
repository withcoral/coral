import classNames from 'classnames'
import type { MouseEvent, ReactNode, Ref, RefObject } from 'react'
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
import { CatalogLayout, CatalogRail, CatalogRailItem, CatalogToolbar } from '@/components/catalog'
import { exactFilter, filterQuery, type FilterQuery } from '@/utils/filter-query'
import { EmptyPage } from '@/components/empty-page'
import {
  PolicyToggle,
  policyLabels,
  policyOptions,
  type PolicyState,
} from '@/components/policy-toggle'
import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import { ActionNotice, Fields } from '@/views/management/forms'
import { ArchivedBanner, trail } from '@/views/management/layout'

import { AddProviderLauncher } from './add-provider'
import { countLabel } from './configuration-details'
import { PolicyMutations, usePolicyMutations } from './policy-mutations'
import {
  encodeSelectedOperation,
  parseSelectionKey,
  SELECTED_OPERATION_FIELD,
  selectionKey,
  type SelectionKey,
} from './selection'
import type {
  FormContext,
  OperationHintsView,
  OperationView,
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
// not declare, so an operation without hints shows no label.
const hintLabels: {
  hint: keyof OperationHintsView
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
    tooltip: 'The provider says this operation changes nothing.',
    falseTooltip: 'The provider says this operation can change data.',
  },
  {
    hint: 'destructive',
    label: 'Destructive',
    declaredFalse: 'Not destructive',
    color: 'red',
    tooltip: 'The provider says this operation can delete or overwrite data.',
    falseTooltip: 'The provider says this operation does not delete or overwrite data.',
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
    tooltip: 'The provider says this operation reaches systems outside the provider.',
    falseTooltip: 'The provider says this operation stays inside the provider.',
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
  hints: OperationHintsView
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
  selectedOperation,
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
  const firstProvider = sortedProviders[0]?.id ?? null
  const operationRoute = /\/providers\/([^/]+)\/ops\/([^/?#]+)/.exec(location.pathname)
  // A provider the URL names, as opposed to the first one standing in for none.
  const routedId = selectedProviderRoute ?? operationRoute?.[1] ?? null
  const selectedId = routedId ?? firstProvider
  const selectedOperationPath =
    selectedOperation ?? (operationRoute?.[2] ? decodeURIComponent(operationRoute[2]) : null)
  const selected = sortedProviders.find((provider) => provider.id === selectedId)
  const operation = selected?.operations.find((o) => o.relativePath === selectedOperationPath)
  const empty = sortedProviders.length === 0
  const addButton = canChange ? <AddProviderLauncher {...entry} /> : null
  // The field echoes each keystroke at once; the rail and the table, which
  // rebuilds thousands of rows, follow with the deferred value when React has time.
  const deferredRailFilter = useDeferredValue(railFilter)
  const filteredProviders = useMemo(() => {
    const query = filterQuery(deferredRailFilter)
    return sortedProviders.filter((i) => query.matches(providerLabel(i)))
  }, [sortedProviders, deferredRailFilter])
  return (
    <PageLayout
      variant="split"
      breadcrumbs={trail([], 'Providers')}
      titled={!empty}
      compact
      header={
        <>
          {!owner && (
            <InfoTip label="Workspace providers">
              As a member, you use the providers. Owners add, remove, and change them.
            </InfoTip>
          )}
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
          addButton={canChange && <AddProviderLauncher {...entry} compact />}
          canChange={canChange}
          filteredProviders={filteredProviders}
          form={form}
          providers={sortedProviders}
          providerIcons={providerIcons}
          railFilter={railFilter}
          selectedProvider={selected ?? null}
          selectedOperation={operation ?? null}
          routedProvider={routedId}
          setRailFilter={setRailFilter}
        />
      )}
    </PageLayout>
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

function sortOperations(operations: OperationView[]) {
  return operations.toSorted((a, b) =>
    caseFolded(a.relativePath).localeCompare(caseFolded(b.relativePath)),
  )
}

type SearchableProvider = {
  provider: ProviderView
  sorted: OperationView[]
}

function searchableProviders(providers: ProviderView[]): SearchableProvider[] {
  return providers.map((provider) => ({
    provider,
    sorted: sortOperations(provider.operations),
  }))
}

/**
 * A summary count the reader clicked: the operations it covered at that
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

function matchingOperations(
  { provider, sorted }: SearchableProvider,
  tableFilter: FilterQuery,
  policyFilter: PolicyFilter | null,
  selectedView: ReadonlySet<SelectionKey> | null,
) {
  // The filter matches only the name the row shows, so every match is visible.
  return sorted.filter((operation) => {
    const key = selectionKey(provider.id, operation.name)
    return (
      tableFilter.matches(operation.relativePath) &&
      (!policyFilter || policyFilter.keys.has(key)) &&
      (!selectedView || selectedView.has(key))
    )
  })
}

/** The rows a provider lists: what matches the filter, plus the operation the
 * drawer shows, which stays in place however the filter reads. It is not a
 * match, so "select all matching" and the hidden count leave it out. */
function listedOperations(
  { sorted }: SearchableProvider,
  matching: OperationView[],
  linkedOperation: OperationView | null,
) {
  if (!linkedOperation || matching.includes(linkedOperation)) return matching
  return sorted.filter((o) => o === linkedOperation || matching.includes(o))
}

type TableRowItem =
  | { kind: 'provider'; key: string; provider: ProviderView }
  | { kind: 'group'; key: string; provider: ProviderView; group: string }
  | { kind: 'operation'; key: string; provider: ProviderView; operation: OperationView }
  | { kind: 'status'; key: string; provider: ProviderView; message: string }

// The same rows as a tree, so the table renders them as nested `Table.Group`s
// and Wax keeps the provider and operation-group headings sticky. `index` is a
// row's position in the flat list the virtualizer measures.
type ProviderSection = {
  provider: ProviderView
  index: number
  operations: OperationView[]
  status: string | null
  statusIndex: number | null
  /** The filter matched none of the provider's operations. */
  noMatch: boolean
  groups: {
    group: string
    index: number
    /** Each member with its index in the flat row list, for the virtualizer. */
    operations: { index: number; operation: OperationView }[]
  }[]
}

// Every heading mounts, so Wax's sticky groups work as they do without a
// virtualizer; only the operation rows inside a group come and go. Each visible
// operation costs a policy form, which is what made the full table slow.
const DENSITY = 'comfortable'
// Provider and group headings are prominent: they stand taller than the operation rows.
const GROUP_HEADINGS = 'prominent'
const HEADING_HEIGHT = groupHeadingHeightPx(DENSITY, GROUP_HEADINGS)
// Until a row mounts and measures itself: an operation row is a cell's height
// plus its border; a provider's status row is as tall as an ordinary row.
const OPERATION_ROW_HEIGHT = CELL_HEIGHT_PX[DENSITY] + 4
const STATUS_ROW_HEIGHT = CELL_HEIGHT_PX[DENSITY]

const tableRowHeight = (row: TableRowItem) => {
  switch (row.kind) {
    case 'provider':
    case 'group':
      return HEADING_HEIGHT
    case 'operation':
      return OPERATION_ROW_HEIGHT
    case 'status':
      return STATUS_ROW_HEIGHT
  }
}

function ProvidersTableScreen({
  addButton,
  canChange,
  filteredProviders,
  form,
  providers,
  providerIcons,
  railFilter,
  selectedProvider,
  selectedOperation,
  routedProvider,
  setRailFilter,
}: {
  /** Adds a provider, beside the page's title. */
  addButton: ReactNode
  canChange: boolean
  filteredProviders: ProviderView[]
  form: FormContext
  providers: ProviderView[]
  /** Recipe logos by namespace, for providers that kept the recipe's id. */
  providerIcons: (provider: ProviderView) => RecipeIcons | undefined
  railFilter: string
  selectedProvider: ProviderView | null
  selectedOperation: OperationView | null
  /** The provider the URL names, which the table reveals on arrival. */
  routedProvider: string | null
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
  const tableRef = useRef<HTMLDivElement>(null)
  const searchable = useMemo(() => searchableProviders(providers), [providers])
  // The rail filter narrows the table to the same providers. Selection is
  // keyed, not row-derived, so operations of a provider filtered out stay
  // selected and count as hidden.
  const listedProviders = useMemo(
    () => new Set(filteredProviders.map((provider) => provider.id)),
    [filteredProviders],
  )
  const visibleByProvider = useMemo(() => {
    const entries = searchable
      .filter((entry) => listedProviders.has(entry.provider.id))
      .map((entry) => {
        const matching = matchingOperations(entry, tableQuery, policyFilter, selectedView)
        const linked = selectedProvider?.id === entry.provider.id ? selectedOperation : null
        return {
          provider: entry.provider,
          matching,
          listed: listedOperations(entry, matching, linked),
        }
      })
    // The selected view lists only the providers it covers.
    if (!selectedView) return entries
    return entries.filter(({ provider }) =>
      provider.operations.some((o) => selectedView.has(selectionKey(provider.id, o.name))),
    )
  }, [
    listedProviders,
    policyFilter,
    searchable,
    selectedOperation,
    selectedProvider?.id,
    selectedView,
    tableQuery,
  ])
  const visibleOperations = visibleByProvider.flatMap(({ provider, matching }) =>
    matching.map((operation) => ({ provider, operation })),
  )
  const allVisibleSelected =
    visibleOperations.length > 0 &&
    visibleOperations.every(({ provider, operation }) =>
      selected.has(selectionKey(provider.id, operation.name)),
    )
  const visibleSelectedCount = visibleOperations.filter(({ provider, operation }) =>
    selected.has(selectionKey(provider.id, operation.name)),
  ).length
  const { rows, sections } = useMemo(() => {
    const flat: TableRowItem[] = []
    const tree: ProviderSection[] = []
    for (const { provider, matching, listed } of visibleByProvider) {
      const section: ProviderSection = {
        provider,
        index: flat.length,
        operations: matching,
        status: null,
        statusIndex: null,
        noMatch: false,
        groups: [],
      }
      tree.push(section)
      flat.push({ kind: 'provider', key: `provider:${provider.id}`, provider })
      if (provider.operations.length === 0) {
        section.status = 'No operations yet. Settings remain available.'
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
        section.status = query ? `No operations match “${query}”.` : 'No matching operations.'
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
      const groups = new Map<string, OperationView[]>()
      for (const operation of listed) {
        const label = operation.group || 'Other'
        const members = groups.get(label)
        if (members) members.push(operation)
        else groups.set(label, [operation])
      }
      for (const [group, members] of [...groups.entries()].toSorted(([a], [b]) =>
        caseFolded(a).localeCompare(caseFolded(b)),
      )) {
        const groupSection: ProviderSection['groups'][number] = {
          group,
          index: flat.length,
          operations: [],
        }
        section.groups.push(groupSection)
        flat.push({ kind: 'group', key: `group:${provider.id}:${group}`, provider, group })
        for (const operation of members) {
          groupSection.operations.push({ index: flat.length, operation })
          flat.push({
            kind: 'operation',
            key: `operation:${provider.id}:${operation.name}`,
            provider,
            operation,
          })
        }
      }
    }
    return { rows: flat, sections: tree }
  }, [policyFilter, selectedView, tableFilter, visibleByProvider])
  const providerIndexes = useMemo(() => {
    const indexes = new Map<string, number>()
    rows.forEach((row, index) => {
      if (row.kind === 'provider') indexes.set(row.provider.id, index)
    })
    return indexes
  }, [rows])
  const operationIndexes = useMemo(() => {
    const indexes = new Map<string, number>()
    rows.forEach((row, index) => {
      if (row.kind === 'operation')
        indexes.set(selectionKey(row.provider.id, row.operation.name), index)
    })
    return indexes
  }, [rows])
  const virtual = Table.useVirtualRows({
    density: DENSITY,
    count: rows.length,
    estimateSize: (index) => tableRowHeight(rows[index]!),
    scrollRef: tableRef,
  })
  const selectedProviderId = selectedProvider?.id ?? null
  const selectedOperationName = selectedOperation?.name ?? null
  const revealProvider = selectedOperationName ? selectedProviderId : routedProvider
  const revealIndex =
    selectedProviderId && selectedOperationName
      ? operationIndexes.get(selectionKey(selectedProviderId, selectedOperationName))
      : routedProvider
        ? providerIndexes.get(routedProvider)
        : undefined
  const markRevealed = useRevealScroll(virtual, revealProvider, selectedOperationName, revealIndex)
  // The table addresses the provider it shows: the URL names the pinned one so
  // the view can be shared. A drawer keeps its own address until it closes.
  const addressProvider = useCallback(
    (provider: ProviderView) => {
      markRevealed(provider.id)
      navigate(provider.path, { replace: true, preventScrollReset: true })
    },
    [markRevealed, navigate],
  )
  const { activeProvider, onPinnedGroupChange } = usePinnedProviderAddress({
    addressOccupied: selectedOperationName !== null,
    addressProvider,
    providers,
    selectedProviderId,
    tableRef,
  })

  // A click reads the policies the summary shows, changes in flight included.
  const policyFilterFor = useCallback(
    (value: PolicyFilter['value'], inFlight: ReturnType<typeof usePolicyMutations>) => {
      const keys = new Set<SelectionKey>()
      for (const provider of providers)
        for (const operation of provider.operations)
          if (
            value === 'unavailable'
              ? !operation.available
              : effectivePolicy(inFlight, provider, operation) === value
          )
            keys.add(selectionKey(provider.id, operation.name))
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

  // The selected view clears the searches, so it shows the whole selection.
  const showSelected = useCallback(() => {
    setTableFilterInput('')
    setRailFilter('')
    setPolicyFilter(null)
    setSelectedView(new Set(selected))
  }, [selected, setRailFilter])

  const toggleOperations = useCallback(
    (targets: { provider: ProviderView; operation: OperationView }[], checked: boolean) => {
      setSelected((current) => {
        const next = new Set(current)
        for (const { provider, operation } of targets) {
          const key = selectionKey(provider.id, operation.name)
          if (checked) next.add(key)
          else next.delete(key)
        }
        return next
      })
    },
    [],
  )
  // The rail is an index into the table: a click scrolls to the provider and
  // names it in the address so the URL can be shared. The route change is a
  // selection within the page, which the parent loader does not revalidate,
  // and the reveal effect scrolls to the same row the click already reached.
  const scrollToProvider = (provider: ProviderView) => {
    const index = providerIndexes.get(provider.id)
    if (index !== undefined) virtual.scrollToIndex(index)
    addressProvider(provider)
  }
  const closeSelectedRoute = () => {
    // The keyboard cursor stays on the operation the drawer showed.
    if (drawerKey) setCursor(drawerKey)
    if (selectedProvider) navigate(selectedProvider.path, { preventScrollReset: true })
  }

  // The keyboard cursor: the operation ↑ and ↓ reach. An open drawer holds it.
  const [cursor, setCursor] = useState<SelectionKey | null>(null)
  const drawerKey =
    selectedProviderId && selectedOperationName
      ? selectionKey(selectedProviderId, selectedOperationName)
      : null
  const activeKey = drawerKey ?? cursor
  const operationOrder = useMemo(
    () =>
      rows.flatMap((row, index) =>
        row.kind === 'operation'
          ? [
              {
                index,
                key: selectionKey(row.provider.id, row.operation.name),
                provider: row.provider,
                operation: row.operation,
              },
            ]
          : [],
      ),
    [rows],
  )
  const activeOperation = operationOrder.find((entry) => entry.key === activeKey)
  const moveCursor =
    (step: 1 | -1, inDrawer = false) =>
    (event: KeyboardEvent) => {
      if (!cursorKeyApplies(event, { fromFilter: true, overControls: true, inDrawer })) return
      event.preventDefault()
      const at = activeOperation ? operationOrder.indexOf(activeOperation) : -1
      const next =
        at < 0
          ? step > 0
            ? operationOrder[0]
            : operationOrder.at(-1)
          : operationOrder[Math.min(Math.max(at + step, 0), operationOrder.length - 1)]
      if (!next || next === activeOperation) return
      setCursor(next.key)
      // An operation sits under its provider and group headings.
      virtual.revealIndex(next.index, { depth: 2 })
      if (drawerKey) {
        markRevealed(next.provider.id, next.operation.name)
        navigate(next.operation.path, { replace: true, preventScrollReset: true })
      }
    }
  // ← and → step the cursor's policy through the toggle's order, stopping at either end.
  const stepPolicy =
    (step: 1 | -1, inDrawer = false) =>
    (event: KeyboardEvent) => {
      if (!activeOperation || !canChange) return
      if (!cursorKeyApplies(event, { fromFilter: false, overControls: false, inDrawer })) return
      event.preventDefault()
      const { provider, operation } = activeOperation
      const mutation = mutations.state(provider.id, operation.name)
      if (mutation.pending) return
      const order = policyOptions.map((option) => option.value)
      const next = order[order.indexOf(mutation.policy ?? operation.policy) + step]
      if (next) mutations.submit(policyFormData(form, provider, operation, next), provider.path)
    }
  const openCursor = (event: KeyboardEvent) => {
    if (drawerKey || !activeOperation) return
    if (!cursorKeyApplies(event, { fromFilter: true, overControls: false })) return
    event.preventDefault()
    markRevealed(activeOperation.provider.id, activeOperation.operation.name)
    navigate(activeOperation.operation.path, { preventScrollReset: true })
  }
  const checkCursor = (event: KeyboardEvent) => {
    if (drawerKey || !activeOperation || !canChange || mutations.bulkPending) return
    if (!cursorKeyApplies(event, { fromFilter: false, overControls: false })) return
    event.preventDefault()
    toggleOperations([activeOperation], !selected.has(activeOperation.key))
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
          empty={filteredProviders.length === 0}
          emptyMessage="No providers match."
        >
          {filteredProviders.map((provider) => (
            <CatalogRailItem
              key={provider.id}
              to={provider.path}
              current={provider.id === activeProvider}
              onSelect={() => scrollToProvider(provider)}
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
              subtitle={provider.displayName ? provider.namespace : undefined}
              trailing={
                <>
                  <Tooltip content={`Show only ${providerLabel(provider)}`}>
                    <Button.Container
                      variant="bare"
                      size="22"
                      ariaLabel={`Show only ${providerLabel(provider)}`}
                      onClick={() => setRailFilter(exactFilter(providerLabel(provider)))}
                    >
                      <Button.Icon name="ListFilter" />
                    </Button.Container>
                  </Tooltip>
                  {/* Coral has no settings: nobody renames, reconfigures, or removes it. */}
                  {!provider.builtin && (
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
                  )}
                </>
              }
            />
          ))}
        </CatalogRail>
      }
    >
      <CatalogToolbar
        title="Providers"
        action={addButton}
        filter={{
          id: OPERATION_FILTER_ID,
          label: 'Filter operations',
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
          ariaLabel="Operations"
          density={DENSITY}
          groupHeadings={GROUP_HEADINGS}
          layout="scroll"
          ref={tableRef}
          onPinnedGroupChange={onPinnedGroupChange}
          className={styles.providersTable}
          columns={[{ width: 44 }, { width: 'minmax(340px, 1fr)' }, { width: 'max-content' }]}
        >
          <Table.Head>
            <Table.Row>
              <span role="columnheader" aria-label="Select" className={styles.policyCell}>
                <Inputs.Checkbox
                  checked={allVisibleSelected}
                  indeterminate={visibleSelectedCount > 0 && !allVisibleSelected}
                  disabled={!canChange || mutations.bulkPending || visibleOperations.length === 0}
                  onCheckedChange={(checked) => toggleOperations(visibleOperations, checked)}
                >
                  <span className={visuallyHidden}>Select all matching operations</span>
                </Inputs.Checkbox>
              </span>
              <span role="columnheader">Operation name</span>
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
            toggleOperations={toggleOperations}
          />
          {/* The rail can bring even a short last provider up under the head. */}
          {sections.length > 0 && (
            <Table.VirtualTail from={sections.at(-1)!.index} virtual={virtual} />
          )}
        </Table.Container>
        {selected.size > 0 && (
          <ActionBar.Bar>
            <BulkPolicyForm
              canChange={canChange}
              form={form}
              providers={providers}
              selected={selected}
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
      <OperationDetailsDrawer
        operation={selectedOperation}
        provider={selectedProvider ?? undefined}
        canChange={canChange}
        form={form}
        onOpenChange={(open) => {
          if (!open) closeSelectedRoute()
        }}
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
 * Arriving on a provider or operation route reveals its row. Only the route's
 * target triggers this: filtering, which moves the row, must not re-scroll,
 * and closing a drawer, which narrows the target to its provider
 * or to none, must leave the table where the reader left it. Returns a
 * function for scrolls the table made itself (rail click, reader scroll), so
 * the address they set does not scroll again.
 */
function useRevealScroll(
  virtual: Table.VirtualRowsHandle,
  provider: string | null,
  operation: string | null,
  index: number | undefined,
) {
  const indexRef = useRef(index)
  indexRef.current = index
  const revealed = useRef<{ provider: string | null; operation: string | null }>({
    provider: null,
    operation: null,
  })
  useEffect(() => {
    const previous = revealed.current
    revealed.current = { provider, operation }
    const unchanged =
      provider === null ||
      (previous.provider === provider && (previous.operation === operation || operation === null))
    if (unchanged) return
    const target = indexRef.current
    // An operation sits under its provider and group headings; a provider under the head.
    if (target !== undefined) virtual.scrollToIndex(target, { depth: operation ? 2 : 0 })
    // oxlint-disable-next-line react-hooks/exhaustive-deps -- The selected route, not the virtual handle, owns this scroll.
  }, [provider, operation])
  return useCallback((providerId: string, operationName: string | null = null) => {
    revealed.current = { provider: providerId, operation: operationName }
  }, [])
}

/**
 * The provider the table shows, from the group heading Wax has pinned. The rail
 * highlights it, and the reader's own scrolling writes it to the address as a
 * replace navigation, settled after scrolling so a fast pass over several
 * providers addresses only where it stops. A reveal scroll on load or after a
 * rail click passes other headings before it settles; addressing one of those
 * would rewrite a shared link or cancel a navigation the reader just started,
 * so only input on the table counts as the reader's scrolling.
 */
function usePinnedProviderAddress({
  addressOccupied,
  addressProvider,
  providers,
  selectedProviderId,
  tableRef,
}: {
  /** A drawer holds the address; scrolling does not take it. */
  addressOccupied: boolean
  addressProvider: (provider: ProviderView) => void
  providers: ProviderView[]
  selectedProviderId: string | null
  tableRef: RefObject<HTMLDivElement | null>
}) {
  const [pinnedProvider, setPinnedProvider] = useState<string | null>(null)
  const onPinnedGroupChange = useCallback(
    (id: string | null) => setPinnedProvider(id === null ? null : providerFromGroupDomId(id)),
    [],
  )
  const readerScrolled = useRef(false)
  useEffect(() => {
    const table = tableRef.current
    if (!table) return
    const mark = () => {
      readerScrolled.current = true
    }
    const events = ['wheel', 'touchmove', 'keydown', 'pointerdown'] as const
    for (const event of events) table.addEventListener(event, mark, { passive: true })
    return () => {
      for (const event of events) table.removeEventListener(event, mark)
    }
  }, [tableRef])
  const pinnedProviderView = pinnedProvider
    ? providers.find((provider) => provider.id === pinnedProvider)
    : undefined
  useEffect(() => {
    if (
      !readerScrolled.current ||
      !pinnedProviderView ||
      addressOccupied ||
      pinnedProviderView.id === selectedProviderId
    )
      return
    const timer = setTimeout(() => addressProvider(pinnedProviderView), 200)
    return () => clearTimeout(timer)
  }, [addressOccupied, addressProvider, pinnedProviderView, selectedProviderId])
  return {
    activeProvider: pinnedProvider ?? selectedProviderId ?? providers[0]?.id ?? '',
    onPinnedGroupChange,
  }
}

const TableSections = memo(function TableSections({
  activeKey,
  bulkPending,
  canChange,
  drawerOpen,
  filterByPolicy,
  form,
  markRevealed,
  policyFilter,
  providerIcons,
  query,
  sections,
  selected,
  showSelected,
  toggleOperations,
  virtual,
}: {
  /** The keyboard cursor's operation, or the drawer's while one is open. */
  activeKey: SelectionKey | null
  bulkPending: boolean
  canChange: boolean
  drawerOpen: boolean
  filterByPolicy: FilterByPolicy
  form: FormContext
  /** Records an operation the reader opened in place, so its route does not scroll the table. */
  markRevealed: (provider: string, operation: string) => void
  policyFilter: PolicyFilter['value'] | null
  providerIcons: (provider: ProviderView) => RecipeIcons | undefined
  /** The operation filter, bolded where it matches a name. */
  query: FilterQuery
  sections: ProviderSection[]
  selected: Set<SelectionKey>
  showSelected: () => void
  toggleOperations: (
    targets: { provider: ProviderView; operation: OperationView }[],
    checked: boolean,
  ) => void
  virtual: Table.VirtualRowsHandle
}) {
  return sections.map((section) => (
    <Table.Group
      key={section.provider.id}
      id={providerGroupDomId(section.provider.id)}
      index={section.index}
      label={providerLabel(section.provider)}
      heading={
        <GroupHeading
          canChange={canChange}
          bulkPending={bulkPending}
          className={styles.providerGroupLabel}
          count={section.operations.length}
          total={section.provider.totalCount}
          label={
            <>
              <StatusDot
                blockers={section.provider.blockers}
                personalAccountAction={section.provider.personalAccountAction}
              />
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
          members={section.operations}
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
          toggleOperations={toggleOperations}
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
          label={group.group}
          heading={
            <GroupHeading
              bulkPending={bulkPending}
              canChange={canChange}
              className={styles.operationGroupLabel}
              count={group.operations.length}
              label={
                <Typography.BodySmallStrong as="span">{group.group}</Typography.BodySmallStrong>
              }
              members={group.operations.map(({ operation }) => operation)}
              name={group.group}
              provider={section.provider}
              selected={selected}
              toggleOperations={toggleOperations}
            />
          }
        >
          <Table.VirtualRows rows={group.operations} virtual={virtual}>
            {({ index, operation }) => (
              <OperationRow
                key={operation.name}
                active={activeKey === selectionKey(section.provider.id, operation.name)}
                drawerOpen={drawerOpen}
                canChange={canChange}
                checked={selected.has(selectionKey(section.provider.id, operation.name))}
                disabled={bulkPending}
                form={form}
                index={index}
                provider={section.provider}
                measure={virtual.measureRow}
                operation={operation}
                query={query}
                onOpen={markRevealed}
                onCheck={(checked) =>
                  toggleOperations([{ provider: section.provider, operation }], checked)
                }
              />
            )}
          </Table.VirtualRows>
        </Table.Group>
      ))}
    </Table.Group>
  ))
})

const OPERATION_FILTER_ID = 'operation-filter'
const PROVIDER_GROUP_DOM_ID = 'provider-group-'

function providerGroupDomId(provider: string) {
  return `${PROVIDER_GROUP_DOM_ID}${provider}`
}

function providerFromGroupDomId(id: string) {
  return id.slice(PROVIDER_GROUP_DOM_ID.length)
}

function providerSettingsDomId(provider: string) {
  return `provider-settings-${provider}`
}

type ToggleOperations = (
  targets: { provider: ProviderView; operation: OperationView }[],
  checked: boolean,
) => void

// The cells of a `Table.Group` heading, provider or operation group: a
// checkbox over the group's matching operations, then a label and count.
function GroupHeading({
  bulkPending,
  canChange,
  count,
  total = count,
  label,
  name,
  members,
  provider,
  selected,
  toggleOperations,
  summary,
  ...cell
}: {
  bulkPending: boolean
  canChange: boolean
  /** The operations the heading counts; the checkbox covers `members`. */
  count: number
  /** Every operation under the heading; while a filter hides some, the heading shows both. */
  total?: number
  label: ReactNode
  /** The accessible name of the checkbox's target. */
  name: string
  members: OperationView[]
  provider: ProviderView
  selected: Set<SelectionKey>
  toggleOperations: ToggleOperations
  /** Closes the label cell, at the heading's end. */
  summary?: ReactNode
  className: string
}) {
  const targets = members.map((operation) => ({ provider, operation }))
  const selectedCount = members.filter((operation) =>
    selected.has(selectionKey(provider.id, operation.name)),
  ).length
  const allSelected = members.length > 0 && selectedCount === members.length
  return (
    <>
      <span role="cell" className={styles.policyCell}>
        <Inputs.Checkbox
          checked={allSelected}
          indeterminate={selectedCount > 0 && !allSelected}
          disabled={!canChange || bulkPending || members.length === 0}
          onCheckedChange={(checked) => toggleOperations(targets, checked)}
        >
          <span className={visuallyHidden}>{`Select ${name} operations`}</span>
        </Inputs.Checkbox>
      </span>
      <span role="cell" className={cell.className}>
        {label}
        <Typography.BodySmall as="span" variant="tertiary">
          {count === total
            ? countLabel(count, 'operation')
            : `${count.toLocaleString('en-US')} of ${countLabel(total, 'operation')}`}
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
  providers,
  selected,
}: {
  canChange: boolean
  form: FormContext
  onClear: () => void
  onConfirmed: (confirmed: SelectionKey[]) => void
  providers: ProviderView[]
  selected: Set<SelectionKey>
}) {
  const mutations = usePolicyMutations()
  const [access, setAccess] = useState<PolicyState>('approval_required')
  // A selected operation with a change in flight (its own row's or a bulk
  // one) holds the form: a second write could target the same operation.
  const saving = [...selected].some((key) => {
    const { provider, operation } = parseSelectionKey(key)
    return mutations.state(provider, operation).pending
  })
  const disabled = !canChange || selected.size === 0 || saving
  // Every provider route serves the same policy action; the selection itself
  // names each operation's provider, so the first route stands for all.
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
        // input per operation: a whole-catalog selection is thousands of
        // inputs to mount and lay out on every selection change.
        const data = new FormData(event.currentTarget)
        for (const key of selected) {
          data.append(SELECTED_OPERATION_FIELD, encodeSelectedOperation(parseSelectionKey(key)))
        }
        mutations.submit(data, anyProviderPath, onConfirmed)
      }}
    >
      <Fields form={form} intent="policy" />
      <input type="hidden" name="access" value={access} />
      <Typography.BodyStrong as="span">{`${selected.size.toLocaleString('en-US')} selected`}</Typography.BodyStrong>
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
            : 'Apply this policy to the selected operations.'
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

const OperationRow = memo(function OperationRow({
  active,
  drawerOpen,
  canChange,
  checked,
  disabled,
  form,
  index,
  provider,
  measure,
  operation,
  query,
  onCheck,
  onOpen,
}: {
  /** The keyboard cursor rests here, or the drawer shows this operation. */
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
  operation: OperationView
  query: FilterQuery
  onCheck: (checked: boolean) => void
  /** Runs before a click opens the operation, which the reader can already see. */
  onOpen: (provider: string, operation: string) => void
}) {
  const unavailableReason = operation.reasons.join(' ') || 'Coral did not provide a reason.'
  const { pending } = usePolicyMutations().state(provider.id, operation.name)
  const navigate = useNavigate()
  // A click anywhere but a control opens the operation's drawer. The name's
  // link navigates itself, so the row leaves its clicks alone.
  const open = (event: MouseEvent<HTMLDivElement>) => {
    const control = (event.target as Element).closest(ROW_CONTROLS + ', a')
    if (control && event.currentTarget.contains(control)) return
    onOpen(provider.id, operation.name)
    navigate(operation.path, { preventScrollReset: true })
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
      <span role="cell" className={styles.policyCell}>
        <Inputs.Checkbox
          checked={checked}
          disabled={!canChange || disabled}
          onCheckedChange={onCheck}
        >
          <span className={visuallyHidden}>{`Select ${operation.relativePath}`}</span>
        </Inputs.Checkbox>
      </span>
      <span role="cell" className={styles.operationNameCell}>
        {/* The row opens the drawer; the name stays a link for the keyboard. */}
        <Tooltip content={operation.relativePath} showOnlyWhenTruncated>
          <Link
            to={operation.path}
            prefetch="intent"
            preventScrollReset
            onClick={() => onOpen(provider.id, operation.name)}
            className={styles.operationName}
          >
            <HighlightedMatches text={operation.relativePath} query={query} />
          </Link>
        </Tooltip>
        <HintLabels hints={operation.hints} />
        {!operation.available && (
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
        <PolicySelect
          canChange={canChange}
          compact
          form={form}
          provider={provider}
          operation={operation}
        />
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
  operation: OperationView,
  access: PolicyState,
) {
  const data = new FormData()
  data.set('csrf', form.csrf)
  data.set('requestId', form.requestId)
  data.set('intent', 'policy')
  data.set('provider', provider.id)
  data.set(
    SELECTED_OPERATION_FIELD,
    encodeSelectedOperation({ provider: provider.id, operation: operation.name }),
  )
  data.set('access', access)
  return data
}

/**
 * Whether a cursor key belongs to the operations table. A control that reads
 * the key keeps it; so does any dialog but the operation drawer, which passes
 * `inDrawer`. With `fromFilter`, the key also works from the operation filter.
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
  // The table scrolls, so it takes focus, but it is not a control in a row.
  if (!target || target === document.body || target.matches('[role="table"]')) return true
  if (target.id === OPERATION_FILTER_ID) return fromFilter
  if (!overControls && target.closest(ROW_CONTROLS + ', a')) return false
  if (target.closest(KEY_CONTROLS)) return false
  return inDrawer || !target.closest('[role="dialog"], [role="alertdialog"]')
}

const PolicySelect = memo(function PolicySelect({
  canChange,
  compact = false,
  form,
  provider,
  operation,
}: {
  canChange: boolean
  /** Sized for a compact table row. */
  compact?: boolean
  form: FormContext
  provider: ProviderView
  operation: OperationView
}) {
  const mutations = usePolicyMutations()
  const mutation = mutations.state(provider.id, operation.name)
  const policy = mutation.policy ?? operation.policy
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
        name={SELECTED_OPERATION_FIELD}
        value={encodeSelectedOperation({ provider: provider.id, operation: operation.name })}
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

/** A provider's no-match status. It counts the selected operations the filter
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
  const hidden = provider.operations.filter((operation) =>
    selected.has(selectionKey(provider.id, operation.name)),
  ).length
  if (hidden === 0) return section.status
  // The copy stays one text run, so its sentences keep their space; the gap
  // only sets the button apart.
  return (
    <span className={styles.sectionStatus}>
      <span>
        {`${section.status} ${hidden} selected ${hidden === 1 ? 'operation is' : 'operations are'} hidden.`}
      </span>
      <Button.Container type="button" variant="secondary" size="22" onClick={onShow}>
        <Button.Icon name="ListFilter" />
        <Button.Text>Show all selected operations</Button.Text>
      </Button.Container>
    </span>
  )
}

type TableFilterValue = PolicyFilter['value'] | 'selected'

/** Narrows the table to the operations one policy covers. It sits beside the search. */
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
      <Menu.Trigger
        render={<Button.Container variant="secondary" ariaLabel="Filter operations by" />}
      >
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
  operation: OperationView,
) {
  return mutations.state(provider.id, operation.name).policy ?? operation.policy
}

/**
 * How many of a provider's operations each policy covers, and how many are
 * unavailable, with the policy changes still in flight. Each count pairs its
 * icon with text, so the summary does not depend on color. A count filters
 * the table to its operations across every provider.
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
  for (const operation of provider.operations) {
    const policy = effectivePolicy(mutations, provider, operation)
    counts.set(policy, (counts.get(policy) ?? 0) + 1)
  }
  const unavailable = provider.totalCount - provider.readyCount
  return (
    <>
      {provider.searchIndex?.state === 'pending' && (
        <Tooltip content="Search cannot find these operations yet. Agents can still call them.">
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
          content={`Search cannot find these operations. Agents can still call them. Reason: ${provider.searchIndex.reason}`}
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
        <Tooltip content="Show only unavailable operations.">
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
        <Tooltip key={value} content={`Show only operations set to ${label}.`}>
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

function OperationDetailsDrawer({
  provider,
  canChange,
  form,
  operation,
  onOpenChange,
  onKeyDown,
}: {
  provider: ProviderView | undefined
  canChange: boolean
  form: FormContext
  operation: OperationView | null
  onOpenChange: (open: boolean) => void
  onKeyDown: (event: KeyboardEvent) => void
}) {
  const drawer = Dialog.useCloseAfterExit(operation?.name ?? null, () => onOpenChange(false))
  // The drawer stays open from one operation to the next, so only its content
  // changes. The content mounts once per operation, which starts it at the top.
  const popup = useRef<HTMLDivElement>(null)
  const scrollToTop = useCallback((content: HTMLDivElement | null) => {
    if (content) popup.current?.scrollTo?.({ top: 0 })
  }, [])
  return (
    <Dialog.Root {...drawer}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup
          ref={popup}
          size="m"
          placement="right"
          onKeyDown={(event) => onKeyDown(event.nativeEvent)}
        >
          <Dialog.Title>{operation?.relativePath ?? 'Operation details'}</Dialog.Title>
          {operation && (
            <div key={operation.name} ref={scrollToTop} className={styles.stack}>
              {operation.description.trim() &&
              operation.description.trim() !== operation.summary.trim() ? (
                <Markdown>{operation.description}</Markdown>
              ) : (
                <Typography.BodySmall as="p">
                  {operation.summary || 'No description.'}
                </Typography.BodySmall>
              )}
              <div className={styles.callPath}>
                <CodeChip>{operation.callPath}</CodeChip>
                <Button.CopyButton
                  textToCopy={operation.callPath}
                  ariaLabel="Copy call path"
                  size="22"
                  variant="bare"
                />
              </div>
              <div className={styles.pills}>
                <HintLabels hints={operation.hints} declaredFalse />
                {provider && (
                  <PolicySelect
                    canChange={canChange}
                    form={form}
                    provider={provider}
                    operation={operation}
                  />
                )}
              </div>
              {provider?.searchIndex?.state === 'pending' && (
                <Banner variant="info" title="Indexing…">
                  Search cannot find this operation yet. Agents can still call it.
                </Banner>
              )}
              {provider?.searchIndex?.state === 'failed' && (
                <Banner variant="info" title="Not in search">
                  Search cannot find this operation. Agents can still call it.
                </Banner>
              )}
              {!operation.available && (
                <Banner variant="warning" title="Unavailable">
                  {operation.reasons.join(' ') || 'Coral did not provide a reason.'}
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
