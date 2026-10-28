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

type Format = 'json' | 'text'
type Range = [start: number, end: number]
/** A piece of the payload Find searches: a key, a scalar or a line. A quoted key or string is
 * searched inside its quotes, so `^` and `$` mean its own start and end, as in a line. */
type Unit = { start: number; text: string; folds: number[]; quoted?: boolean }
/** One match: the ranges, within its unit's text, that its pieces matched. */
type Match = { unit: number; ranges: Range[] }
type Mode = 'structured' | 'raw'
/** What the expanded viewer opens with: the view you had. */
type ViewState = { mode: Mode; wrap: boolean; folded: Set<number>; query: string; current: number }

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
  const matches = useMemo(() => findMatches(units, search), [search, units])
  const body = useRef<HTMLDivElement>(null)
  // The expanded viewer opens on the match you were at.
  const revealed = useRef(Boolean(initial?.query))
  /** Moves to match `index` of `found` in `inUnits`, unfolding whatever hides it. */
  const go = (index: number, found: Match[], inUnits: Unit[]) => {
    setCurrent(index)
    const match = found[index]
    if (!match) return
    const hidden = inUnits[match.unit]!.folds
    if (hidden.some((fold) => folded.has(fold)))
      setFolded((before) => new Set([...before].filter((fold) => !hidden.includes(fold))))
    revealed.current = true
  }
  useEffect(() => {
    if (!revealed.current) return
    revealed.current = false
    body.current?.querySelector('[data-current-match]')?.scrollIntoView?.({ block: 'nearest' })
  })
  const step = (by: number) =>
    matches.length && go((current + by + matches.length) % matches.length, matches, units)
  /** The marks of each unit, each with the index of the match it belongs to. */
  const marks = useMemo(() => {
    const byUnit = new Map<number, { range: Range; index: number }[]>()
    matches.forEach((match, index) => {
      const list = byUnit.get(match.unit) ?? []
      for (const range of match.ranges) list.push({ range, index })
      byUnit.set(match.unit, list)
    })
    return byUnit
  }, [matches])
  const unitIndex = useMemo(() => new Map(units.map((unit, index) => [unit.start, index])), [units])
  /** `text`, the unit starting at `start`, with its matches marked; `shown` cuts it short. */
  const marked = (start: number, text: string, shown = text.length): ReactNode => {
    const list = marks.get(unitIndex.get(start) ?? -1)
    if (!list) return text.slice(0, shown)
    const parts: ReactNode[] = []
    let from = 0
    for (const { range, index } of list) {
      const [a, b] = [Math.min(range[0], shown), Math.min(range[1], shown)]
      if (a >= b) continue
      if (a > from) parts.push(text.slice(from, a))
      parts.push(
        <mark
          key={a}
          className={classNames(styles.mark, index === current && styles.currentMark)}
          data-current-match={index === current ? '' : undefined}
        >
          {text.slice(a, b)}
        </mark>,
      )
      from = b
    }
    if (from < shown) parts.push(text.slice(from, shown))
    return parts
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
            const nextUnits = unitsFor(next === 'structured')
            go(0, findMatches(nextUnits, search), nextUnits)
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
              go(0, findMatches(units, filterQuery(next)), units)
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
  marked: (start: number, text: string, shown?: number) => ReactNode
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
    const size = `${count} ${nouns[node.kind][count === 1 ? 0 : 1]}`
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
                ? node.entries.map((entry, index) => (
                    <JsonValue
                      key={entry.keyStart}
                      node={entry.value}
                      name={keyName(entry.key)}
                      keyText={entry.key}
                      keyStart={entry.keyStart}
                      trailing={index < count - 1 ? ',' : ''}
                      folded={folded}
                      toggle={toggle}
                      marked={marked}
                    />
                  ))
                : node.items.map((item, index) => (
                    <JsonValue
                      key={item.start}
                      node={item}
                      name={`Item ${index + 1}`}
                      keyText={null}
                      trailing={index < count - 1 ? ',' : ''}
                      folded={folded}
                      toggle={toggle}
                      marked={marked}
                    />
                  ))}
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
  const shown = isFolded ? STRING_PREVIEW_LENGTH : node.raw.length
  return (
    <div className={styles.line}>
      <span className={styles.toggleSpacer} />
      <span className={styles.lineText}>
        {key}
        <span className={node.kind === 'string' ? token.string : token.constant}>
          {marked(node.start, node.raw, shown)}
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
            aria-label={`${isFolded ? `${node.raw.length - shown} more characters` : 'Fold'}: ${name}`}
            onClick={() => toggle(node.start)}
          >
            {isFolded ? `${node.raw.length - shown} more characters` : 'Fold'}
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

/** Every place `search` matches in `units`, in order. */
function findMatches(units: Unit[], search: FilterQuery): Match[] {
  if (!search.text) return []
  return units.flatMap((unit, index) => {
    const text = unit.quoted ? unit.text.slice(1, -1) : unit.text
    const shift = unit.quoted ? 1 : 0
    return search.hits(text).map((ranges): Match => ({
      unit: index,
      ranges: ranges.map(([a, b]): Range => [a + shift, b + shift]),
    }))
  })
}

/** Each key and scalar, with the folds that hide it. */
function structureUnits(structure: JsonNode): Unit[] {
  const units: Unit[] = []
  const visit = (node: JsonNode, folds: number[]) => {
    if (node.kind === 'object') {
      const inner = [...folds, node.start]
      for (const entry of node.entries) {
        units.push({ start: entry.keyStart, text: entry.key, folds: inner, quoted: true })
        visit(entry.value, inner)
      }
    } else if (node.kind === 'array') {
      for (const item of node.items) visit(item, [...folds, node.start])
    } else
      units.push({
        start: node.start,
        text: node.raw,
        folds:
          node.kind === 'string' && node.raw.length > LONG_STRING_LENGTH
            ? [...folds, node.start]
            : folds,
        quoted: node.kind === 'string',
      })
  }
  visit(structure, [])
  return units
}

/** Each line, at its exact offset, as `sourceLines` counts them. */
function lineUnits(value: string): Unit[] {
  let start = 0
  const units: Unit[] = []
  for (const terminator of value.matchAll(LINE_TERMINATOR)) {
    units.push({ start, text: value.slice(start, terminator.index), folds: [] })
    start = terminator.index + terminator[0].length
  }
  units.push({ start, text: value.slice(start), folds: [] })
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
