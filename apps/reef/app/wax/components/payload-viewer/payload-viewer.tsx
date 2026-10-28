import { useVirtualizer } from '@tanstack/react-virtual'
import classNames from 'classnames'
import { useEffect, useEffectEvent, useMemo, useRef, useState, type ReactNode } from 'react'

import { filterQuery } from '@/utils/filter-query'
import * as Button from '@/wax/components/button'
import { CopyButton } from '@/wax/components/button/copy-button'
import { token } from '@/wax/components/code-block/code-block.css'
import * as Dialog from '@/wax/components/dialog'
import { HighlightedMatches, type Highlight } from '@/wax/components/highlighted-matches'
import { TextInput } from '@/wax/components/inputs/text/text-input'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { Typography } from '@/wax/components/typography'

import { jsonStructure, valueCount, type JsonNode } from './json-structure'
import {
  findMatches,
  lineUnits,
  LONG_STRING_LENGTH,
  structureUnits,
  type Range,
  type Unit,
} from './payload-search'
import * as styles from './payload-viewer.css'

export { LONG_STRING_LENGTH } from './payload-search'

/** Past either bound, JSON opens shallow: its top level shows, deeper containers fold. */
export const LARGE_PAYLOAD_LENGTH = 4096
export const LARGE_PAYLOAD_VALUES = 100
const STRING_PREVIEW_LENGTH = 120
/** A row's height until it mounts and measures itself, and the body's padding above and below. */
const ROW_ESTIMATE = 22
const BODY_PADDING = 12
/** Each level of a structure indents its rows this much. */
const INDENT = 18

type Format = 'json' | 'text'
type Mode = 'structured' | 'raw'
/** What the expanded viewer opens with: the view you had. */
type ViewState = { mode: Mode; wrap: boolean; folded: Set<number>; query: string; current: number }
/** One line of a structure: a value, with its key before it and `trailing` (a comma) after
 * it, or the line that closes an open container. */
type Row =
  | {
      kind: 'value'
      node: JsonNode
      depth: number
      /** What the value's toggle names it: its key, its index, or "Value" at the top. */
      name: string
      keyText: string | null
      keyStart?: number
      trailing: string
    }
  | { kind: 'close'; node: JsonNode; depth: number; trailing: string }

export interface PayloadViewerProps {
  /** The payload exactly as retained. */
  value: string
  format: Format
  /** Names the payload's region and its expanded viewer. */
  label: string
  /** The payload holds only the start of a longer value. */
  truncated?: boolean
  /** The download's file name; defaults to the label. */
  fileName?: string
  /** Called with true when the expanded viewer opens, and with false once it has closed and
   * focus is back on Expand, or when it unmounts open. A page can hold still meanwhile. */
  onExpandedChange?: (expanded: boolean) => void
}

/**
 * One retained payload, exact. Inline, a card shows it, foldable, and scrolls it past a
 * maximum height; Copy, Download and Expand sit above the card, at its right. Expand opens
 * the viewer with every inspection tool: type and retained size, Structured or Raw, Find and
 * Wrap. JSON shows as a foldable structure built from its own tokens, or exactly as retained
 * in Raw; text keeps its lines, numbered. Only the rows near the view mount, so a huge
 * payload scrolls whole.
 */
export function PayloadViewer(props: PayloadViewerProps) {
  return <Viewer {...props} expanded={false} />
}

function Viewer({
  value,
  format,
  label,
  truncated = false,
  fileName,
  onExpandedChange,
  expanded,
  initial,
}: PayloadViewerProps & { expanded: boolean; initial?: ViewState }) {
  const [open, setOpen] = useState(false)
  // From the open until the close has ended, so focus is back on Expand before a page moves.
  const [held, setHeld] = useState(false)
  const reportExpanded = useEffectEvent((next: boolean) => onExpandedChange?.(next))
  // The cleanup also runs when the viewer unmounts open, so no page stays held.
  useEffect(() => {
    if (!held) return
    reportExpanded(true)
    return () => reportExpanded(false)
  }, [held])
  const parsed = useMemo(() => (format === 'json' ? jsonStructure(value) : null), [format, value])
  const structure = typeof parsed === 'object' ? parsed : null
  const large = useMemo(
    () =>
      structure !== null &&
      (value.length > LARGE_PAYLOAD_LENGTH || valueCount(structure) > LARGE_PAYLOAD_VALUES),
    [structure, value],
  )
  const size = useMemo(() => byteSize(value), [value])
  const [mode, setMode] = useState<Mode>(initial?.mode ?? 'structured')
  const [wrap, setWrap] = useState(initial?.wrap ?? true)
  const [folded, setFolded] = useState(() => initial?.folded ?? initialFolds(structure, large))
  const [query, setQuery] = useState(initial?.query ?? '')
  const [current, setCurrent] = useState(initial?.current ?? 0)
  const structured = structure !== null && mode === 'structured'
  const search = useMemo(() => filterQuery(query), [query])
  const parts = useMemo(() => (structure ? structureUnits(structure) : null), [structure])
  const lines = useMemo(() => lineUnits(value), [value])
  /** The units Find searches in a view, and what else it searches there. */
  const viewOf = (structuredView: boolean) =>
    structuredView && parts
      ? { units: parts.units, within: { source: value, entries: parts.entries } }
      : { units: lines, within: undefined }
  const units = structured && parts ? parts.units : lines
  const { matches, rawOnly } = useMemo(
    () =>
      findMatches(
        units,
        search,
        structured && parts ? { source: value, entries: parts.entries } : undefined,
      ),
    [units, search, structured, parts, value],
  )
  const rows = useMemo(
    () => (structured && structure ? structureRows(structure, folded) : null),
    [structured, structure, folded],
  )
  /** By unit offset, the row that shows it. */
  const rowOf = useMemo(() => {
    const at = new Map<number, number>()
    if (rows)
      rows.forEach((row, index) => {
        if (row.kind !== 'value') return
        if (row.keyStart !== undefined) at.set(row.keyStart, index)
        if (row.node.kind !== 'object' && row.node.kind !== 'array') at.set(row.node.start, index)
      })
    else lines.forEach((line, index) => at.set(line.start, index))
    return at
  }, [rows, lines])
  const body = useRef<HTMLDivElement>(null)
  // oxlint-disable-next-line react/incompatible-library -- TanStack Virtual owns scroll measurements; only its rows and sizes cross into render.
  const virtualizer = useVirtualizer({
    count: rows ? rows.length : lines.length,
    getScrollElement: () => body.current,
    // Before the body lays out, as on the server, the first rows render, so they show at once.
    initialRect: { width: 800, height: 720 },
    estimateSize: () => ROW_ESTIMATE,
    getItemKey: (index) => (rows ? rowKey(rows[index]!) : lines[index]!.start),
    paddingStart: BODY_PADDING,
    paddingEnd: BODY_PADDING,
    overscan: 20,
  })
  // The match to scroll to after the next render, and then its mark, once it has mounted.
  // The expanded viewer opens on the match you were at.
  const reveal = useRef<'row' | 'mark' | null>(initial?.query ? 'row' : null)
  useEffect(() => {
    if (reveal.current === 'row') {
      const match = matches[current]
      const row = match && rowOf.get(units[match.units[0]!]!.start)
      reveal.current = row === undefined ? null : 'mark'
      if (row !== undefined) virtualizer.scrollToIndex(row, { align: 'center' })
    } else if (reveal.current === 'mark') {
      const mark = body.current?.querySelector('[aria-current="true"]')
      if (!mark) return
      reveal.current = null
      mark.scrollIntoView?.({ block: 'nearest' })
    }
  })
  /** Moves to match `index` of `found` in `inUnits`, unfolding whatever hides it. */
  const go = (index: number, found: typeof matches, inUnits: Unit[]) => {
    setCurrent(index)
    const match = found[index]
    if (!match) return
    const hidden = match.units.flatMap((unit) => inUnits[unit]!.folds)
    if (hidden.some((fold) => folded.has(fold)))
      setFolded((before) => new Set([...before].filter((fold) => !hidden.includes(fold))))
    reveal.current = 'row'
  }
  const step = (by: number) =>
    matches.length && go((current + by + matches.length) % matches.length, matches, units)
  /** The found parts of each unit, within its text, each with the match it belongs to. */
  const marks = useMemo(() => {
    const byUnit = new Map<number, { range: Range; index: number }[]>()
    matches.forEach((match, index) => {
      for (const unit of match.units) {
        const { start, text } = units[unit]!
        const list = byUnit.get(unit) ?? []
        for (const [a, b] of match.ranges) {
          const range: Range = [
            Math.max(a, start) - start,
            Math.min(b, start + text.length) - start,
          ]
          if (range[0] < range[1]) list.push({ range, index })
        }
        byUnit.set(unit, list)
      }
    })
    return byUnit
  }, [matches, units])
  const unitIndex = useMemo(() => new Map(units.map((unit, index) => [unit.start, index])), [units])
  /** `text`, the unit starting at `start`, with its matches marked; `cut` cuts it short. */
  const marked = (start: number, text: string, cut = text.length): ReactNode => {
    const highlights: Highlight[] = (marks.get(unitIndex.get(start) ?? -1) ?? [])
      .filter(({ range }) => range[0] < cut)
      .map(({ range, index }) => ({
        range: [range[0], Math.min(range[1], cut)],
        current: index === current,
      }))
    return <HighlightedMatches text={text.slice(0, cut)} highlights={highlights} />
  }
  const toggle = (start: number) =>
    setFolded((before) => {
      const next = new Set(before)
      if (!next.delete(start)) next.add(start)
      return next
    })
  const kind = format === 'text' ? 'Text' : parsed === 'invalid' ? 'Not valid JSON' : 'JSON'
  const download = () => {
    const url = URL.createObjectURL(
      new Blob([value], { type: format === 'json' ? 'application/json' : 'text/plain' }),
    )
    const link = document.createElement('a')
    link.href = url
    link.download = fileName ?? `${slug(label)}.${format === 'json' ? 'json' : 'txt'}`
    link.click()
    setTimeout(() => URL.revokeObjectURL(url))
  }
  // What Structured cannot show, such as text across two items, Find counts as Raw's.
  const findStatus = !search.text
    ? ''
    : (matches.length
        ? `${Math.min(current, matches.length - 1) + 1} of ${matches.length}`
        : 'No matches') + (rawOnly ? ` · ${rawOnly} ${matches.length ? 'more ' : ''}in Raw` : '')
  // Copy, Download and, inline, Expand: compact, above the card or in the viewer's toolbar.
  const actions = (
    <div className={styles.actions} role="group" aria-label="Payload actions">
      <CopyButton
        size="22"
        variant="bare"
        ariaLabel={`Copy ${label}`}
        tooltipText="Copy"
        textToCopy={value}
      />
      <Button.IconButton
        name="Download"
        size="22"
        variant="bare"
        ariaLabel={`Download ${label}`}
        tooltipText="Download"
        onClick={download}
      />
      {!expanded && (
        <Dialog.Root
          open={open}
          onOpenChange={(next) => {
            setOpen(next)
            if (next) setHeld(true)
          }}
          onOpenChangeComplete={(next) => {
            // A close that ends after you opened it again leaves the hold.
            if (!next && !open) setHeld(false)
          }}
        >
          <Dialog.Trigger
            render={<Button.Container size="22" variant="bare" ariaLabel={`Expand ${label}`} />}
          >
            <Button.Icon name="Maximize2" />
          </Dialog.Trigger>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup size="xxl" className={styles.expandedPopup}>
              {/* Wax's standard header: the title, and its close control at the top right. */}
              <Dialog.Header>
                <Dialog.Title>{label}</Dialog.Title>
                <Dialog.Close />
              </Dialog.Header>
              <Viewer
                value={value}
                format={format}
                label={label}
                truncated={truncated}
                fileName={fileName}
                expanded
                initial={{ mode, wrap, folded, query, current }}
              />
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      )}
    </div>
  )
  // The expanded viewer's tools; the inline card keeps only its actions.
  const toolbar = (
    <div className={styles.toolbar} role="group" aria-label="Payload tools">
      <Typography.BodySmall variant="tertiary" className={styles.facts}>
        {kind} · {size}
      </Typography.BodySmall>
      {structure && (
        <SegmentedControl
          label="View"
          size="small"
          options={[
            { value: 'structured', label: 'Structured' },
            { value: 'raw', label: 'Raw' },
          ]}
          value={mode}
          onChange={(next) => {
            setMode(next)
            const view = viewOf(next === 'structured')
            go(0, findMatches(view.units, search, view.within).matches, view.units)
          }}
        />
      )}
      {!structured && (
        <Button.TextButton
          size="22"
          variant="secondary"
          aria-pressed={wrap}
          onClick={() => setWrap(!wrap)}
        >
          Wrap
        </Button.TextButton>
      )}
      <div className={styles.find}>
        <div className={styles.findInput}>
          <TextInput
            type="search"
            icon="Search"
            ariaLabel={`Find in ${label}`}
            placeholder="Find"
            value={query}
            onChange={(next) => {
              setQuery(next)
              const view = viewOf(structured)
              go(0, findMatches(view.units, filterQuery(next), view.within).matches, view.units)
            }}
            onKeyDown={(event) => {
              if (event.key !== 'Enter') return
              event.preventDefault()
              step(event.shiftKey ? -1 : 1)
            }}
          />
        </div>
        <Typography.BodySmall variant="tertiary" role="status">
          {findStatus}
        </Typography.BodySmall>
        <Button.IconButton
          name="ChevronUp"
          size="22"
          variant="bare"
          ariaLabel="Previous match"
          disabled={!matches.length}
          onClick={() => step(-1)}
        />
        <Button.IconButton
          name="ChevronDown"
          size="22"
          variant="bare"
          ariaLabel="Next match"
          disabled={!matches.length}
          onClick={() => step(1)}
        />
      </div>
      {actions}
    </div>
  )
  /** The rows near the view, each at its place in the whole; only they mount. */
  const virtual = (
    name: string,
    className: string | false,
    render: (index: number) => ReactNode,
  ) => (
    <div
      role="group"
      aria-label={name}
      className={classNames(styles.rows, className)}
      style={{ blockSize: virtualizer.getTotalSize() }}
    >
      {virtualizer.getVirtualItems().map((item) => (
        <div
          key={item.key}
          data-index={item.index}
          ref={virtualizer.measureElement}
          className={classNames(styles.row, !structured && !wrap && styles.unwrappedRow)}
          style={{ transform: `translateY(${item.start}px)` }}
        >
          {render(item.index)}
        </div>
      ))}
    </div>
  )
  const content = rows
    ? virtual('Structure', styles.wrapped, (index) => (
        <StructureRow row={rows[index]!} folded={folded} toggle={toggle} marked={marked} />
      ))
    : format === 'text'
      ? virtual('Lines', false, (index) => (
          <div
            className={styles.numbered}
            style={{ gridTemplateColumns: `${String(lines.length).length}ch minmax(0, 1fr)` }}
          >
            <span className={styles.lineNumber} aria-hidden>
              {index + 1}
            </span>
            <span className={classNames(styles.lineText, wrap ? styles.wrapped : styles.unwrapped)}>
              {marked(lines[index]!.start, lines[index]!.text)}
            </span>
          </div>
        ))
      : virtual('Raw', false, (index) => {
          const line = lines[index]!
          return (
            <div className={classNames(styles.rawLine, wrap ? styles.wrapped : styles.unwrapped)}>
              {marked(line.start, line.text)}
              {/* Its own terminator, unseen, so the text stays exact. */}
              <span className={styles.terminator}>
                {value.slice(
                  line.start + line.text.length,
                  lines[index + 1]?.start ?? value.length,
                )}
              </span>
            </div>
          )
        })
  const notice = truncated && (
    <Typography.BodySmall>This is only the start of the value.</Typography.BodySmall>
  )
  if (!expanded)
    return (
      <section className={styles.viewer} aria-label={label}>
        {notice}
        <div className={styles.inlineActions}>{actions}</div>
        {/* A long payload scrolls in its bordered card; Tab reaches it to scroll by key. */}
        <div
          ref={body}
          role="group"
          aria-label="Payload content"
          tabIndex={0}
          className={classNames(styles.body, styles.framedBody, styles.inlineBody)}
        >
          {content}
        </div>
      </section>
    )
  return (
    <section className={classNames(styles.viewer, styles.expandedViewer)}>
      {toolbar}
      {notice}
      <div
        ref={body}
        className={classNames(
          styles.body,
          styles.framedBody,
          !structured && !wrap && styles.sideways,
          styles.expandedBody,
        )}
      >
        {content}
      </div>
    </section>
  )
}

/** Each line of `structure`, its folded containers closed, in order. */
function structureRows(structure: JsonNode, folded: Set<number>): Row[] {
  const rows: Row[] = []
  const visit = (
    node: JsonNode,
    depth: number,
    name: string,
    keyText: string | null,
    keyStart: number | undefined,
    trailing: string,
  ) => {
    rows.push({ kind: 'value', node, depth, name, keyText, keyStart, trailing })
    if (node.kind !== 'object' && node.kind !== 'array') return
    const count = node.kind === 'object' ? node.entries.length : node.items.length
    if (!count || folded.has(node.start)) return
    if (node.kind === 'object')
      node.entries.forEach((entry, index) =>
        visit(
          entry.value,
          depth + 1,
          keyName(entry.key),
          entry.key,
          entry.keyStart,
          index < count - 1 ? ',' : '',
        ),
      )
    else
      node.items.forEach((item, index) =>
        visit(item, depth + 1, `Item ${index + 1}`, null, undefined, index < count - 1 ? ',' : ''),
      )
    rows.push({ kind: 'close', node, depth, trailing })
  }
  visit(structure, 0, 'Value', null, undefined, '')
  return rows
}

const rowKey = (row: Row) => `${row.kind} ${row.node.start}`

const nouns = { object: ['property', 'properties'], array: ['item', 'items'] } as const

/** One row of a structure. */
function StructureRow({
  row,
  folded,
  toggle,
  marked,
}: {
  row: Row
  folded: Set<number>
  toggle: (start: number) => void
  marked: (start: number, text: string, cut?: number) => ReactNode
}) {
  const { node } = row
  const indent = { paddingInlineStart: row.depth * INDENT }
  const container = node.kind === 'object' || node.kind === 'array'
  const [open, close] = node.kind === 'object' ? ['{', '}'] : ['[', ']']
  if (row.kind === 'close')
    return (
      <div className={styles.line} style={indent}>
        <span className={styles.toggleSpacer} />
        <span>
          {close}
          {row.trailing}
        </span>
      </div>
    )
  const key = row.keyText !== null && (
    <>
      {marked(row.keyStart!, row.keyText)}
      {': '}
    </>
  )
  if (container) {
    const count = node.kind === 'object' ? node.entries.length : node.items.length
    if (count === 0)
      return (
        <div className={styles.line} style={indent}>
          <span className={styles.toggleSpacer} />
          <span className={styles.lineText}>
            {key}
            {open}
            {close}
            {row.trailing}
          </span>
        </div>
      )
    const isFolded = folded.has(node.start)
    const size = `${count} ${nouns[node.kind][count === 1 ? 0 : 1]}`
    return (
      <div className={styles.line} style={indent}>
        <Button.IconButton
          className={styles.toggle}
          name={isFolded ? 'ChevronRight' : 'ChevronDown'}
          size="22"
          variant="bare"
          ariaLabel={`${row.name}: ${size}`}
          aria-expanded={!isFolded}
          onClick={() => toggle(node.start)}
        />
        <span className={styles.lineText}>
          {key}
          {open}
          {isFolded ? (
            <>
              {' … '}
              {close}
              {row.trailing} <span className={styles.summary}>{size}</span>
            </>
          ) : (
            <span className={styles.summary}> {size}</span>
          )}
        </span>
      </div>
    )
  }
  const raw = 'raw' in node ? node.raw : ''
  const long = node.kind === 'string' && raw.length > LONG_STRING_LENGTH
  const isFolded = long && folded.has(node.start)
  const cut = isFolded ? STRING_PREVIEW_LENGTH : raw.length
  return (
    <div className={styles.line} style={indent}>
      <span className={styles.toggleSpacer} />
      <span className={styles.lineText}>
        {key}
        <span className={node.kind === 'string' ? token.string : token.constant}>
          {marked(node.start, raw, cut)}
          {isFolded && '…'}
        </span>
        {row.trailing}
        {long && (
          <Button.TextButton
            className={styles.moreButton}
            size="22"
            variant="bare"
            aria-expanded={!isFolded}
            // The name starts with the visible label, then says which string it is.
            aria-label={`${isFolded ? `${raw.length - cut} more characters` : 'Fold'}: ${row.name}`}
            onClick={() => toggle(node.start)}
          >
            {isFolded ? `${raw.length - cut} more characters` : 'Fold'}
          </Button.TextButton>
        )}
      </span>
    </div>
  )
}

/** Containers below the top level and long strings, in a large payload; nothing otherwise. */
function initialFolds(structure: JsonNode | null, large: boolean) {
  const folds = new Set<number>()
  if (!structure || !large) return folds
  const visit = (node: JsonNode, depth: number) => {
    if (node.kind === 'object' || node.kind === 'array') {
      if (depth > 0) folds.add(node.start)
      for (const child of node.kind === 'object'
        ? node.entries.map((entry) => entry.value)
        : node.items)
        visit(child, depth + 1)
    } else if (node.kind === 'string' && node.raw.length > LONG_STRING_LENGTH) folds.add(node.start)
  }
  visit(structure, 0)
  return folds
}

/** A key as you read it, for a toggle's name: its escapes read, its quotes gone. */
function keyName(raw: string) {
  try {
    return JSON.parse(raw) as string
  } catch {
    return raw
  }
}

const slug = (label: string) =>
  label
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '') || 'payload'

/** The retained payload's UTF-8 size, rounded only for display. */
export function byteSize(value: string) {
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 1000) return `${bytes} B`
  const units = ['kB', 'MB', 'GB']
  let size = bytes / 1000
  let unit = 0
  while (Number(size.toPrecision(3)) >= 1000 && unit < units.length - 1) {
    size /= 1000
    unit++
  }
  return `${Number(size.toPrecision(3))} ${units[unit]}`
}
