import { useEffect, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'
import { Link } from 'react-router'

import { Icon } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'
import type { FilterQuery } from '@/utils/filter-query'

import * as styles from './tree.css'

export interface TreeNode {
  /** Stable identity; also the DOM id suffix. */
  id: string
  label: string
  /** Secondary text after the label, such as a count. */
  detail?: string
  /** Show the current child count instead of static secondary text. */
  countChildren?: boolean
  /** Leaf search fields for filterNodes; defaults to the label. Branches match through children. */
  searchTerms?: readonly string[]
  /** Where selecting this node navigates. A node without one only expands or collapses. */
  href?: string
  /** Renders the node as unavailable. */
  dimmed?: boolean
  /** Leaf marker: solid when the leaf is usable, hollow when it is not. */
  marker?: 'solid' | 'hollow' | 'none'
  /** A slot at the end of the row, such as a pill that qualifies the node. */
  trailing?: ReactNode
  children?: readonly TreeNode[]
}

export interface TreeProps {
  /** The accessible name of the tree. */
  label: string
  nodes: readonly TreeNode[]
  /** Nodes that start collapsed. Everything else starts expanded. */
  defaultCollapsedIds?: readonly string[]
  /** The collapsed nodes, when the page drives expansion itself, such as from a
   * search box or an Expand all control. Pass it with `onCollapsedChange`. */
  collapsedIds?: readonly string[]
  onCollapsedChange?: (ids: readonly string[]) => void
  /** Shown when there are no nodes, for instance after filtering. */
  emptyMessage?: string
}

/** Leaves whose search terms match; a branch survives through its children. */
export function filterNodes(nodes: readonly TreeNode[], query: FilterQuery): readonly TreeNode[] {
  if (!query.text) return nodes
  return nodes.flatMap((node) => {
    if (node.children) {
      const children = filterNodes(node.children, query)
      return children.length ? [{ ...node, children }] : []
    }
    return query.matches(...(node.searchTerms ?? [node.label])) ? [node] : []
  })
}

interface VisibleNode {
  node: TreeNode
  level: number
  parentId: string | null
  expanded: boolean
}

function flatten(
  nodes: readonly TreeNode[],
  collapsed: ReadonlySet<string>,
  level: number,
  parentId: string | null,
  into: VisibleNode[],
) {
  for (const node of nodes) {
    const expanded = Boolean(node.children?.length) && !collapsed.has(node.id)
    into.push({ node, level, parentId, expanded })
    if (expanded) flatten(node.children!, collapsed, level + 1, node.id, into)
  }
  return into
}

/**
 * A keyboard-navigable ARIA tree. A node with an `href` is a link; expansion
 * is the only state the tree owns.
 */
export function Tree({
  label,
  nodes,
  defaultCollapsedIds,
  collapsedIds,
  onCollapsedChange,
  emptyMessage,
}: TreeProps) {
  const [own, setOwn] = useState(() => new Set(defaultCollapsedIds ?? []))
  // Expansion is the tree's own state until a page claims it.
  const controlled = collapsedIds !== undefined
  const collapsed = controlled ? new Set(collapsedIds) : own
  const setCollapsed = (next: (current: ReadonlySet<string>) => Set<string>) => {
    if (controlled) onCollapsedChange?.([...next(collapsed)])
    else setOwn(next)
  }
  const [focusedId, setFocusedId] = useState<string>()
  const pendingFocus = useRef(false)
  const items = useRef(new Map<string, HTMLLIElement>())
  const links = useRef(new Map<string, HTMLElement>())
  const visible = flatten(nodes, collapsed, 1, null, [])
  const activeId =
    focusedId && visible.some(({ node }) => node.id === focusedId) ? focusedId : visible[0]?.node.id
  useEffect(() => {
    if (pendingFocus.current && activeId) {
      pendingFocus.current = false
      items.current.get(activeId)?.focus()
    }
  }, [activeId])
  const toggle = (id: string) =>
    setCollapsed((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  const moveTo = (id: string | undefined) => {
    if (!id) return
    pendingFocus.current = true
    setFocusedId(id)
  }
  const onKeyDown = (event: KeyboardEvent<HTMLUListElement>) => {
    const index = visible.findIndex(({ node }) => node.id === activeId)
    if (index < 0) return
    const current = visible[index]
    const hasChildren = Boolean(current.node.children?.length)
    switch (event.key) {
      case 'ArrowDown':
        moveTo(visible[index + 1]?.node.id)
        break
      case 'ArrowUp':
        moveTo(visible[index - 1]?.node.id)
        break
      case 'Home':
        moveTo(visible[0]?.node.id)
        break
      case 'End':
        moveTo(visible.at(-1)?.node.id)
        break
      case 'ArrowRight':
        if (hasChildren && !current.expanded) toggle(current.node.id)
        else if (hasChildren) moveTo(visible[index + 1]?.node.id)
        break
      case 'ArrowLeft':
        if (hasChildren && current.expanded) toggle(current.node.id)
        else moveTo(current.parentId ?? undefined)
        break
      case 'Enter':
      case ' ':
        if (current.node.href) links.current.get(current.node.id)?.click()
        else if (hasChildren) toggle(current.node.id)
        break
      default:
        return
    }
    event.preventDefault()
  }
  if (visible.length === 0)
    return (
      <div role="tree" aria-label={label} className={styles.tree}>
        <div className={styles.empty}>{emptyMessage ?? 'Nothing to show'}</div>
      </div>
    )
  const render = (nodesToRender: readonly TreeNode[], level: number) =>
    nodesToRender.map((node) => {
      const hasChildren = Boolean(node.children?.length)
      const expanded = hasChildren && !collapsed.has(node.id)
      const active = node.id === activeId
      return (
        <li
          key={node.id}
          role="treeitem"
          aria-level={level}
          aria-expanded={hasChildren ? expanded : undefined}
          tabIndex={active ? 0 : -1}
          className={styles.item}
          ref={(element) => {
            if (element) items.current.set(node.id, element)
            else items.current.delete(node.id)
          }}
          onFocus={(event) => {
            if (event.target === event.currentTarget) setFocusedId(node.id)
          }}
        >
          <div
            data-row
            className={styles.row({ dimmed: Boolean(node.dimmed) })}
            style={{ paddingInlineStart: (level - 1) * 16 + 4 }}
          >
            {hasChildren ? (
              <button
                type="button"
                tabIndex={-1}
                className={styles.toggle}
                aria-label={expanded ? `Collapse ${node.label}` : `Expand ${node.label}`}
                onClick={() => toggle(node.id)}
              >
                <Icon name={expanded ? 'ChevronDown' : 'ChevronRight'} size="16" color="inherit" />
              </button>
            ) : (
              <span className={styles.marker({ kind: node.marker ?? 'solid' })} aria-hidden />
            )}
            {node.href ? (
              <Link
                to={node.href}
                tabIndex={-1}
                className={styles.label}
                ref={(element) => {
                  if (element) links.current.set(node.id, element)
                  else links.current.delete(node.id)
                }}
                onClick={() => setFocusedId(node.id)}
              >
                {node.label}
              </Link>
            ) : (
              <button
                type="button"
                tabIndex={-1}
                className={styles.label}
                onClick={() => {
                  setFocusedId(node.id)
                  if (hasChildren) toggle(node.id)
                }}
              >
                {node.label}
              </button>
            )}
            {node.countChildren ? (
              <span className={styles.detail}>{node.children?.length ?? 0}</span>
            ) : (
              // The summary is what the row had to give up room for, so hovering
              // it is how you read the part that did not fit.
              node.detail && (
                <Tooltip content={node.detail} showOnlyWhenTruncated>
                  <span className={styles.summary}>{node.detail}</span>
                </Tooltip>
              )
            )}
            {node.trailing && <span className={styles.trailing}>{node.trailing}</span>}
          </div>
          {expanded && (
            <ul role="group" className={styles.group}>
              {render(node.children!, level + 1)}
            </ul>
          )}
        </li>
      )
    })
  return (
    <ul role="tree" aria-label={label} className={styles.tree} onKeyDown={onKeyDown}>
      {render(nodes, 1)}
    </ul>
  )
}
