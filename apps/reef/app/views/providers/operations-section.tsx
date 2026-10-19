import { useMemo, useState, type ReactNode } from 'react'

import { Banner, Button, Inputs, ScrollArea, Typography } from '@/wax/components'
import { Tree, filterNodes, type TreeNode } from '@/wax/components/tree'

import {
  DestinationList,
  countLabel,
  destinationGroups,
  operationCount,
} from './configuration-details'
import { REFRESH_NEW, REFRESH_REMOVED, reviewOperationNodes } from './tree-model'
import { filterQuery } from '@/utils/filter-query'
import type { LiveOperationView, SetupView } from './types'
import * as styles from './providers.css'

/** Every group node, so the list opens closed and one control can open it all. */
function groupIds(nodes: readonly TreeNode[]) {
  return nodes.filter((node) => node.children?.length).map((node) => node.id)
}

/**
 * The previewed catalog, grouped by the provider's own tags and collapsed. You
 * see the shape of it first and open only the group you care about. A search
 * opens the groups it leaves behind, so a match is never hidden.
 *
 * The panel is the same before there is a list: a provider that has to be
 * signed into shows why in place of the rows, in the frame the rows will fill,
 * so the step does not change shape when they arrive.
 */
export function OperationsBrowser({
  setup,
  previous,
  empty,
}: {
  /** Absent before a source is read; `empty` then fills the panel. */
  setup?: SetupView
  previous?: readonly LiveOperationView[]
  empty?: ReactNode
}) {
  const nodes = useMemo(
    () => (setup ? reviewOperationNodes(setup, previous) : []),
    [setup, previous],
  )
  const groups = useMemo(() => groupIds(nodes), [nodes])
  const [query, setQuery] = useState('')
  // New and Removed open at the start: they are what a refresh changes.
  const [collapsed, setCollapsed] = useState<readonly string[]>(() =>
    groups.filter((id) => id !== REFRESH_NEW && id !== REFRESH_REMOVED),
  )
  const search = useMemo(() => filterQuery(query), [query])
  const shown = useMemo(() => filterNodes(nodes, search), [nodes, search])
  const open = search.text ? [] : collapsed
  const expanded = open.length === 0
  return (
    <div className={styles.operationsBrowser}>
      <div className={styles.operationsSearch}>
        {/* Before the search box, not after it: this label changes width, and
            the box would shift under the pointer every time it did. */}
        {(groups.length > 0 || empty) && (
          <Button.TextButton
            type="button"
            variant="secondary"
            disabled={Boolean(search.text) || Boolean(empty)}
            onClick={() => setCollapsed(expanded ? groups : [])}
          >
            {/* Nothing is open when there is nothing to open. */}
            {!empty && expanded ? 'Collapse all' : 'Expand all'}
          </Button.TextButton>
        )}
        <div className={styles.operationsFind}>
          <Inputs.TextInput
            type="search"
            icon="Search"
            ariaLabel="Find an operation"
            placeholder="Find an operation"
            value={query}
            onChange={setQuery}
            disabled={Boolean(empty)}
          />
        </div>
      </div>
      {/* Nothing to scroll while the panel is empty, so the message takes the
          room the rows will have and sits in the middle of it. */}
      {empty ? (
        <div className={styles.operationsEmpty}>{empty}</div>
      ) : (
        <ScrollArea.Container
          maxHeight="420px"
          constrainWidth
          scrollDirection="vertical"
          className={styles.operationsScroll}
        >
          <Tree
            label="Operations"
            nodes={shown}
            collapsedIds={open}
            // A search decides what is open; collapsing one group then would be
            // undone by the next keystroke.
            onCollapsedChange={search.text ? undefined : setCollapsed}
            emptyMessage="No operations match your search."
          />
        </ScrollArea.Container>
      )}
    </div>
  )
}

/** How many operations the source offers, beside the step's title. */
export function OperationsSummary({
  setup,
  discovering,
}: {
  setup: SetupView
  discovering: boolean
}) {
  const review = setup.configuration
  const operations = operationCount(setup)
  if (!review || operations === null) return null
  const destinations = destinationGroups(setup)
  const groups = new Set(review.operations.map((operation) => operation.group)).size
  const browsable = !discovering && review.operations.length > 0
  return (
    <Typography.BodySmall as="p" variant="tertiary">
      {[
        countLabel(operations, 'operation') +
          (browsable && groups > 1 ? ` in ${countLabel(groups, 'group')}` : ''),
        // One destination is the unremarkable case: the count only earns
        // its place when calls are split between several.
        destinations.size > 1 ? countLabel(destinations.size, 'destination') : '',
      ]
        .filter(Boolean)
        .join(' · ')}
    </Typography.BodySmall>
  )
}

/**
 * What the source turned out to offer: how many operations, which of them
 * cannot run under the confirmed configuration, and where each one's calls go.
 * The operations step asks about this; the last step only states the decisions.
 */
export function OperationsSection({
  setup,
  previous,
  discovery,
}: {
  setup: SetupView
  /** The live provider's operations, so a refresh can group what changes. */
  previous?: readonly LiveOperationView[]
  /** Shown inside the panel when Coral cannot read the list yet. */
  discovery?: ReactNode
}) {
  const review = setup.configuration
  if (!review) return null
  const destinations = destinationGroups(setup)
  // The browser puts each group's destination on its own row and holds every
  // operation. Without one, the destinations and the failures need listing.
  const browsable = !discovery && review.operations.length > 0
  return (
    <section className={styles.stack} aria-label="Operations">
      {review.unavailableOperations.length > 0 && (
        <Banner variant="warning">
          {countLabel(review.unavailableOperations.length, 'operation')} cannot run with this
          configuration. Open the Cannot run group to see why.
        </Banner>
      )}
      {review.diagnostic && <Banner variant="warning">{review.diagnostic}</Banner>}
      {discovery ? (
        <OperationsBrowser setup={setup} empty={discovery} />
      ) : browsable ? (
        <OperationsBrowser setup={setup} previous={previous} />
      ) : (
        destinations.size > 0 && (
          <>
            <Typography.BodySmallStrong as="h3">Where requests go</Typography.BodySmallStrong>
            <DestinationList setup={setup} />
          </>
        )
      )}
    </section>
  )
}
