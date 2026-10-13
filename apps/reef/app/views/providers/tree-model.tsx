import { Icon } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'
import type { TreeNode } from '@/wax/components/tree'

import { countLabel } from './configuration-details'
import type { LiveOperationView, OperationNodeView, ReviewOperationView, SetupView } from './types'

/**
 * A refresh's own groups. Outside `group:`, so a provider tag named `new` or
 * `removed` cannot share their IDs.
 */
export const REFRESH_NEW = 'refresh:new'
export const REFRESH_REMOVED = 'refresh:removed'

/** The path under its group node, so `actions.listRunners` reads `listRunners` beneath `actions`. */
function leafLabel(o: OperationNodeView) {
  const head = `${o.group}.`
  return o.group && o.relativePath.toLowerCase().startsWith(head.toLowerCase())
    ? o.relativePath.slice(head.length)
    : o.relativePath
}

function operationLeaf(o: OperationNodeView): TreeNode {
  return {
    id: o.relativePath,
    label: leafLabel(o),
    searchTerms: [o.relativePath, o.summary, o.group],
    // A Setup has nothing to select: its Operations have no page until it publishes.
    href: o.path || undefined,
    dimmed: !o.available,
    marker: o.available ? 'solid' : 'hollow',
  }
}

/** Leaves grouped by tag or dotted prefix; a flat list when nothing groups. */
function operationNodes(operations: readonly OperationNodeView[]): TreeNode[] {
  const groups = new Map<string, OperationNodeView[]>()
  for (const o of operations) groups.set(o.group, [...(groups.get(o.group) ?? []), o])
  if (groups.size === 1 && groups.has('')) return operations.map(operationLeaf)
  return [...groups.entries()].map(([group, members]) => ({
    id: `group:${group}`,
    label: group || 'Other',
    countChildren: true,
    children: members.map(operationLeaf),
  }))
}

/** The host a group's Operations agree on, or how many they split between. */
function groupDestination(
  members: readonly ReviewOperationView[],
  destinations: Record<string, string>,
) {
  const urls = new Set(members.map((o) => destinations[o.key]).filter(Boolean))
  if (urls.size === 0) return ''
  if (urls.size === 1) return safeHost([...urls][0])
  return countLabel(urls.size, 'destination')
}

function safeHost(url: string) {
  try {
    return new URL(url).host
  } catch {
    return url
  }
}

/** Why one Operation cannot run, on the row itself rather than beside it. */
function blockedMarker(reasons: readonly string[]) {
  return (
    <Tooltip content={reasons.join(' ') || 'This operation cannot run with this configuration.'}>
      <span aria-label="Cannot run" role="img">
        <Icon name="TriangleAlert" size="14" color="warning" />
      </span>
    </Tooltip>
  )
}

/**
 * The previewed catalog as the operations step reads it: the provider's own tags
 * become the groups, each one saying how many Operations it holds and where
 * their calls go. Whatever cannot run is lifted into one group above them all,
 * because that is the part of the list a person has to act on.
 */
export function reviewOperationNodes(
  setup: SetupView,
  /** The live provider's operations. A refresh groups what it adds and removes. */
  previous?: readonly LiveOperationView[],
): TreeNode[] {
  const review = setup.configuration
  if (!review) return []
  const known = previous && new Set(previous.map((o) => o.relativePath))
  const read = new Set(review.operations.map((o) => o.relativePath))
  const removed = previous?.filter((o) => !read.has(o.relativePath)) ?? []
  const added = known ? review.operations.filter((o) => !known.has(o.relativePath)) : []
  const blocked = review.operations.filter((o) => !o.available)
  const runnable = review.operations.filter(
    (o) => o.available && !(known && !known.has(o.relativePath)),
  )
  const byPath = new Map(review.operations.map((o) => [o.relativePath, o]))
  const decorate = (node: TreeNode): TreeNode => {
    const operation = byPath.get(node.id)
    if (!operation) return node
    return {
      ...node,
      detail: operation.summary,
      trailing: operation.available ? undefined : blockedMarker(operation.reasons),
    }
  }
  const groupNode = (node: TreeNode): TreeNode => {
    if (!node.children) return decorate(node)
    const members = node.children
      .map((child) => byPath.get(child.id))
      .filter((o): o is ReviewOperationView => Boolean(o))
    return {
      ...node,
      countChildren: false,
      detail: [
        countLabel(members.length, 'operation'),
        groupDestination(members, review.destinations),
      ]
        .filter(Boolean)
        .join(' \u00b7 '),
      children: node.children.map(decorate),
    }
  }
  const nodes = operationNodes(runnable).map(groupNode)
  // What a refresh adds, above the provider's own groups, so the change is the
  // first thing you see. One that cannot run stays in Cannot run.
  const fresh = added.filter((o) => o.available)
  if (fresh.length > 0)
    nodes.unshift({
      id: REFRESH_NEW,
      label: 'New',
      detail: countLabel(fresh.length, 'operation'),
      children: fresh.map((o) =>
        decorate({
          id: o.relativePath,
          label: o.relativePath,
          searchTerms: [o.relativePath, o.summary, o.group],
          marker: 'solid',
        }),
      ),
    })
  // What a refresh removes, after New: it has no page or destination any more.
  if (removed.length > 0)
    nodes.splice(fresh.length > 0 ? 1 : 0, 0, {
      id: REFRESH_REMOVED,
      label: 'Removed',
      dimmed: true,
      detail: countLabel(removed.length, 'operation'),
      children: removed.map((o) => ({
        id: `removed:${o.relativePath}`,
        label: o.relativePath,
        detail: o.summary,
        searchTerms: [o.relativePath, o.summary],
        dimmed: true,
        marker: 'hollow',
      })),
    })
  if (blocked.length === 0) return nodes
  // Its own group, above the rest: a tag says what an Operation is for, and
  // these are grouped by the one thing the person still has to settle.
  return [
    {
      id: 'group:unavailable',
      label: 'Cannot run',
      dimmed: true,
      detail: countLabel(blocked.length, 'operation'),
      children: blocked.map((o) =>
        decorate({
          id: o.relativePath,
          label: o.relativePath,
          searchTerms: [o.relativePath, o.summary, o.group],
          dimmed: true,
          marker: 'hollow',
        }),
      ),
    },
    ...nodes,
  ]
}
