import classNames from 'classnames'
import {
  Fragment,
  useEffect,
  useEffectEvent,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'

import { filterQuery, type FilterQuery } from '@/utils/filter-query'
import * as Button from '@/wax/components/button'
import { CopyButton } from '@/wax/components/button/copy-button'
import { token } from '@/wax/components/code-block/code-block.css'
import { LINE_TERMINATOR } from '@/wax/components/code-block/lines'
import * as Dialog from '@/wax/components/dialog'
import { HighlightedMatches, type Highlight } from '@/wax/components/highlighted-matches'
import { TextInput } from '@/wax/components/inputs/text/text-input'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { Typography } from '@/wax/components/typography'

import { jsonStructure, valueCount, type JsonNode } from './json-structure'
import * as styles from './payload-viewer.css'

/** Past either bound, JSON opens shallow: its top level shows, deeper containers fold. */
export const LARGE_PAYLOAD_LENGTH = 4096
export const LARGE_PAYLOAD_VALUES = 100
/** A string longer than this can fold to its start; in a large payload it starts folded. */
export const LONG_STRING_LENGTH = 200
const STRING_PREVIEW_LENGTH = 120
/** A container shows this many children at first, and this many more each time you ask. */
export const CHILD_WINDOW = 200

type Format = 'json' | 'text'
type Range = [start: number, end: number]
/** Where a value sits: its container's offset, and its index among the container's children. */
type Place = [container: number, index: number]
/** A piece of the payload Find searches: a key, a scalar or a line. A quoted key or string is
 * searched inside its quotes, so `^` and `$` mean its own start and end, as in a line. A key
 * and its scalar value share an `entry`; any other unit is an entry alone. */
type Unit = {
  start: number
  text: string
  folds: number[]
  places: Place[]
  entry: number
  quoted?: boolean
}
/** One match: the ranges of the payload its pieces matched, and the units it shows in. */
type Match = { ranges: Range[]; units: number[] }
type Mode = 'structured' | 'raw'
/** What the expanded viewer opens with: the view you had. */
type ViewState = {
  mode: Mode
  wrap: boolean
  folded: Set<number>
  shown: Map<number, number>
  query: string
  current: number
}

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
 * maximum height; Copy, Download and Expand sit in the card's top-right corner. Expand opens
 * the viewer with every inspection tool: type and retained size, Structured or Raw, Find and
 * Wrap. JSON shows as a foldable structure built from its own tokens, or exactly as retained
 * in Raw; text keeps its lines, numbered.
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
  // By container, how many of its children show, when that is more than the first window.
  const [shown, setShown] = useState(() => initial?.shown ?? new Map<number, number>())
  const [query, setQuery] = useState(initial?.query ?? '')
  const [current, setCurrent] = useState(initial?.current ?? 0)
  const structured = structure !== null && mode === 'structured'
  const search = useMemo(() => filterQuery(query), [query])
  const unitsFor = (structuredView: boolean) =>
    structuredView && structure ? structureUnits(structure) : lineUnits(value)
  const units = useMemo(
    () => (structured && structure ? structureUnits(structure) : lineUnits(value)),
    [structured, structure, value],
  )
  // Structured, Find also searches the payload as retained, across keys and values.
  const sourceFor = (structuredView: boolean) => (structuredView ? value : null)
  const matches = useMemo(
    () => findMatches(units, search, structured ? value : null),
    [search, units, structured, value],
  )
  const body = useRef<HTMLDivElement>(null)
  // The expanded viewer opens on the match you were at.
  const revealed = useRef(Boolean(initial?.query))
  /** Moves to match `index` of `found` in `inUnits`, unfolding and showing whatever hides it. */
  const go = (index: number, found: Match[], inUnits: Unit[]) => {
    setCurrent(index)
    const match = found[index]
    if (!match) return
    const touched = match.units.map((unit) => inUnits[unit]!)
    const hidden = touched.flatMap((unit) => unit.folds)
    if (hidden.some((fold) => folded.has(fold)))
      setFolded((before) => new Set([...before].filter((fold) => !hidden.includes(fold))))
    const places = touched.flatMap((unit) => unit.places)
    if (places.some(([container, at]) => at >= (shown.get(container) ?? CHILD_WINDOW)))
      setShown((before) => {
        const next = new Map(before)
        for (const [container, at] of places) {
          const window = Math.ceil((at + 1) / CHILD_WINDOW) * CHILD_WINDOW
          next.set(container, Math.max(next.get(container) ?? CHILD_WINDOW, window))
        }
        return next
      })
    revealed.current = true
  }
  useEffect(() => {
    if (!revealed.current) return
    revealed.current = false
    body.current?.querySelector('[data-current-match]')?.scrollIntoView?.({ block: 'nearest' })
  })
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
  /** Shows the next window of the container at `start`'s children. */
  const more = (start: number) =>
    setShown((before) =>
      new Map(before).set(start, (before.get(start) ?? CHILD_WINDOW) + CHILD_WINDOW),
    )
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
  const findStatus = !search.text
    ? ''
    : matches.length
      ? `${Math.min(current, matches.length - 1) + 1} of ${matches.length}`
      : 'No matches'
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
            if (!next) setHeld(false)
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
                initial={{ mode, wrap, folded, shown, query, current }}
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
            const nextUnits = unitsFor(next === 'structured')
            go(0, findMatches(nextUnits, search, sourceFor(next === 'structured')), nextUnits)
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
              go(0, findMatches(units, filterQuery(next), sourceFor(structured)), units)
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
  const content = structured ? (
    <div role="group" aria-label="Structure" className={styles.wrapped}>
      <JsonValue
        node={structure}
        name="Value"
        keyText={null}
        trailing=""
        folded={folded}
        toggle={toggle}
        shown={shown}
        more={more}
        marked={marked}
      />
    </div>
  ) : format === 'text' ? (
    <div role="group" aria-label="Lines" className={styles.numbered}>
      {units.map((unit, index) => (
        <LineRow key={unit.start} number={index + 1} wrap={wrap}>
          {marked(unit.start, unit.text)}
        </LineRow>
      ))}
    </div>
  ) : (
    <div role="group" aria-label="Raw">
      <pre className={classNames(styles.raw, wrap ? styles.wrapped : styles.unwrapped)}>
        {/* Each line with its matches, then its own terminator, so the text stays exact. */}
        {units.map((unit, index) => (
          <Fragment key={unit.start}>
            {marked(unit.start, unit.text)}
            {value.slice(unit.start + unit.text.length, units[index + 1]?.start ?? value.length)}
          </Fragment>
        ))}
      </pre>
    </div>
  )
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

function LineRow({
  number,
  wrap,
  children,
}: {
  number: number
  wrap: boolean
  children: ReactNode
}) {
  return (
    <>
      <span className={styles.lineNumber} aria-hidden>
        {number}
      </span>
      <span className={classNames(styles.lineText, wrap ? styles.wrapped : styles.unwrapped)}>
        {children}
      </span>
    </>
  )
}

const nouns = { object: ['property', 'properties'], array: ['item', 'items'] } as const

/** One value, its key before it and `trailing` (a comma) after it. */
function JsonValue({
  node,
  name,
  keyText,
  keyStart,
  trailing,
  folded,
  toggle,
  shown,
  more,
  marked,
}: {
  node: JsonNode
  /** What the value's toggle names it: its key, its index, or "Value" at the top. */
  name: string
  keyText: string | null
  keyStart?: number
  trailing: string
  folded: Set<number>
  toggle: (start: number) => void
  shown: Map<number, number>
  more: (start: number) => void
  marked: (start: number, text: string, cut?: number) => ReactNode
}) {
  const key = keyText !== null && (
    <>
      {marked(keyStart!, keyText)}
      {': '}
    </>
  )
  if (node.kind === 'object' || node.kind === 'array') {
    const count = node.kind === 'object' ? node.entries.length : node.items.length
    const [open, close] = node.kind === 'object' ? ['{', '}'] : ['[', ']']
    if (count === 0)
      return (
        <div className={styles.line}>
          <span className={styles.toggleSpacer} />
          <span className={styles.lineText}>
            {key}
            {open}
            {close}
            {trailing}
          </span>
        </div>
      )
    const isFolded = folded.has(node.start)
    const noun = (n: number) => nouns[node.kind][n === 1 ? 0 : 1]
    const size = `${count} ${noun(count)}`
    // A huge container shows its children a window at a time, and says how many follow.
    const limit = shown.get(node.start) ?? CHILD_WINDOW
    const rest = count - limit
    const next = Math.min(rest, CHILD_WINDOW)
    const passed = { folded, toggle, shown, more, marked }
    return (
      <div>
        <div className={styles.line}>
          <Button.IconButton
            className={styles.toggle}
            name={isFolded ? 'ChevronRight' : 'ChevronDown'}
            size="22"
            variant="bare"
            ariaLabel={`${name}: ${size}`}
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
                {trailing} <span className={styles.summary}>{size}</span>
              </>
            ) : (
              <span className={styles.summary}> {size}</span>
            )}
          </span>
        </div>
        {!isFolded && (
          <>
            <div className={styles.children}>
              {node.kind === 'object'
                ? node.entries
                    .slice(0, limit)
                    .map((entry, index) => (
                      <JsonValue
                        key={entry.keyStart}
                        node={entry.value}
                        name={keyName(entry.key)}
                        keyText={entry.key}
                        keyStart={entry.keyStart}
                        trailing={index < count - 1 ? ',' : ''}
                        {...passed}
                      />
                    ))
                : node.items
                    .slice(0, limit)
                    .map((item, index) => (
                      <JsonValue
                        key={item.start}
                        node={item}
                        name={`Item ${index + 1}`}
                        keyText={null}
                        trailing={index < count - 1 ? ',' : ''}
                        {...passed}
                      />
                    ))}
              {rest > 0 && (
                <div className={styles.line}>
                  <span className={styles.toggleSpacer} />
                  <span className={styles.lineText}>
                    <span className={styles.summary}>
                      {rest} more {noun(rest)}
                    </span>
                    <Button.TextButton
                      className={styles.moreButton}
                      size="22"
                      variant="bare"
                      // The name starts with the visible label, then says which container.
                      aria-label={`Show next ${next}: ${name}`}
                      onClick={() => more(node.start)}
                    >
                      Show next {next}
                    </Button.TextButton>
                  </span>
                </div>
              )}
            </div>
            <div className={styles.line}>
              <span className={styles.toggleSpacer} />
              <span>
                {close}
                {trailing}
              </span>
            </div>
          </>
        )}
      </div>
    )
  }
  const long = node.kind === 'string' && node.raw.length > LONG_STRING_LENGTH
  const isFolded = long && folded.has(node.start)
  const cut = isFolded ? STRING_PREVIEW_LENGTH : node.raw.length
  return (
    <div className={styles.line}>
      <span className={styles.toggleSpacer} />
      <span className={styles.lineText}>
        {key}
        <span className={node.kind === 'string' ? token.string : token.constant}>
          {marked(node.start, node.raw, cut)}
          {isFolded && '…'}
        </span>
        {trailing}
        {long && (
          <Button.TextButton
            className={styles.moreButton}
            size="22"
            variant="bare"
            aria-expanded={!isFolded}
            // The name starts with the visible label, then says which string it is.
            aria-label={`${isFolded ? `${node.raw.length - cut} more characters` : 'Fold'}: ${name}`}
            onClick={() => toggle(node.start)}
          >
            {isFolded ? `${node.raw.length - cut} more characters` : 'Fold'}
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

/**
 * Every place `search` matches in `units`, in order. With `source`, the payload as retained,
 * it also matches across one entry's units, as a key with its value, or a quoted token. A
 * match that overlaps one found in a unit, or that reaches another entry, is not counted.
 */
function findMatches(units: Unit[], search: FilterQuery, source: string | null): Match[] {
  if (!search.text) return []
  let found = units.flatMap((unit) => {
    const text = unit.quoted ? unit.text.slice(1, -1) : unit.text
    const shift = unit.start + (unit.quoted ? 1 : 0)
    return search.hits(text).map((ranges) => ranges.map(([a, b]): Range => [a + shift, b + shift]))
  })
  if (source !== null) {
    // Matches in units never overlap one another, so their spans are in order.
    const spans = found.map((ranges): Range => [ranges[0]![0], ranges.at(-1)![1]])
    const overlaps = ([start, end]: Range) => {
      let low = 0
      let high = spans.length
      while (low < high) {
        const middle = (low + high) >> 1
        if (spans[middle]![0] < end) low = middle + 1
        else high = middle
      }
      return low > 0 && spans[low - 1]![1] > start
    }
    for (const line of lineUnits(source))
      for (const ranges of search.hits(line.text)) {
        const shifted = ranges.map(([a, b]): Range => [a + line.start, b + line.start])
        if (shifted.length && !overlaps([shifted[0]![0], shifted.at(-1)![1]])) found.push(shifted)
      }
    found = found.toSorted((a, b) => a[0]![0] - b[0]![0])
  }
  return found.flatMap((ranges) => {
    const touched = touching(units, ranges)
    const entry = units[touched[0] ?? -1]?.entry
    return entry !== undefined && touched.every((unit) => units[unit]!.entry === entry)
      ? [{ ranges, units: touched }]
      : []
  })
}

/** The indexes of the units, in offset order, that `ranges` overlap. */
function touching(units: Unit[], ranges: Range[]) {
  const touched = new Set<number>()
  for (const [start, end] of ranges) {
    let low = 0
    let high = units.length
    while (low < high) {
      const middle = (low + high) >> 1
      if (units[middle]!.start <= start) low = middle + 1
      else high = middle
    }
    for (let at = Math.max(low - 1, 0); at < units.length && units[at]!.start < end; at++)
      if (units[at]!.start + units[at]!.text.length > start) touched.add(at)
  }
  return [...touched].toSorted((a, b) => a - b)
}

/** Each key and scalar, in offset order, with the folds and windows that hide it. */
function structureUnits(structure: JsonNode): Unit[] {
  const units: Unit[] = []
  const visit = (node: JsonNode, folds: number[], places: Place[], entry = node.start) => {
    if (node.kind === 'object') {
      const inner = [...folds, node.start]
      node.entries.forEach((item, index) => {
        const at: Place[] = [...places, [node.start, index]]
        units.push({
          start: item.keyStart,
          text: item.key,
          folds: inner,
          places: at,
          entry: item.keyStart,
          quoted: true,
        })
        visit(item.value, inner, at, item.keyStart)
      })
    } else if (node.kind === 'array') {
      node.items.forEach((item, index) =>
        visit(item, [...folds, node.start], [...places, [node.start, index]]),
      )
    } else
      units.push({
        start: node.start,
        text: node.raw,
        folds:
          node.kind === 'string' && node.raw.length > LONG_STRING_LENGTH
            ? [...folds, node.start]
            : folds,
        places,
        entry,
        quoted: node.kind === 'string',
      })
  }
  visit(structure, [], [])
  return units
}

/** Each line, at its exact offset, as `sourceLines` counts them. */
function lineUnits(value: string): Unit[] {
  let start = 0
  const units: Unit[] = []
  for (const terminator of value.matchAll(LINE_TERMINATOR)) {
    units.push({
      start,
      text: value.slice(start, terminator.index),
      folds: [],
      places: [],
      entry: start,
    })
    start = terminator.index + terminator[0].length
  }
  units.push({ start, text: value.slice(start), folds: [], places: [], entry: start })
  return units
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
