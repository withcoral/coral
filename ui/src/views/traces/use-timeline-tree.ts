import { useCallback, useEffect, useMemo, useState } from 'react'

import { type TraceSpan } from '@/generated/coral/v1/traces_pb'

import { sortedSpans } from './trace-utils'

export interface TimelineNode {
  children: TimelineNode[]
  span: TraceSpan
}

export interface TimelineRow {
  childCount: number
  depth: number
  span: TraceSpan
}

function compareSpanStart(a: TraceSpan, b: TraceSpan): number {
  const aStart = BigInt(a.startTimeUnixNanos || 0)
  const bStart = BigInt(b.startTimeUnixNanos || 0)
  if (aStart < bStart) return -1
  if (aStart > bStart) return 1
  return a.spanId.localeCompare(b.spanId)
}

function compareTimelineNodes(a: TimelineNode, b: TimelineNode): number {
  return compareSpanStart(a.span, b.span)
}

export function buildTimelineTree(spans: TraceSpan[], rootSpanId?: string): TimelineNode[] {
  const ordered = sortedSpans(spans)
  const nodesById = new Map<string, TimelineNode>()

  for (const span of ordered) nodesById.set(span.spanId, { children: [], span })

  const roots: TimelineNode[] = []
  for (const span of ordered) {
    const node = nodesById.get(span.spanId)
    if (!node) continue

    const parent = span.parentSpanId ? nodesById.get(span.parentSpanId) : undefined
    if (parent) parent.children.push(node)
    else roots.push(node)
  }

  for (const node of nodesById.values()) node.children.sort(compareTimelineNodes)

  const rootNode = rootSpanId ? nodesById.get(rootSpanId) : undefined
  if (!rootNode) return roots.sort(compareTimelineNodes)

  return [rootNode, ...roots.filter((node) => node.span.spanId !== rootSpanId).sort(compareTimelineNodes)]
}

export function flattenVisibleTimelineTree(nodes: TimelineNode[], collapsedSpanIds: Set<string>): TimelineRow[] {
  const rows: TimelineRow[] = []
  const visited = new Set<string>()

  const visit = (node: TimelineNode, depth: number) => {
    if (visited.has(node.span.spanId)) return
    visited.add(node.span.spanId)
    rows.push({ childCount: node.children.length, depth, span: node.span })
    if (collapsedSpanIds.has(node.span.spanId)) return
    for (const child of node.children) visit(child, depth + 1)
  }

  for (const node of nodes) visit(node, 0)
  return rows
}

export function useTimelineTree(spans: TraceSpan[], rootSpanId?: string, traceId?: string) {
  const [collapsedSpanIds, setCollapsedSpanIds] = useState<Set<string>>(() => new Set())

  useEffect(() => setCollapsedSpanIds(new Set()), [traceId])

  const toggleSpan = useCallback((spanId: string) => {
    setCollapsedSpanIds((current) => {
      const next = new Set(current)
      if (next.has(spanId)) next.delete(spanId)
      else next.add(spanId)
      return next
    })
  }, [])

  const tree = useMemo(() => buildTimelineTree(spans, rootSpanId), [rootSpanId, spans])
  const rows = useMemo(() => flattenVisibleTimelineTree(tree, collapsedSpanIds), [collapsedSpanIds, tree])

  return { collapsedSpanIds, rows, toggleSpan, tree }
}
