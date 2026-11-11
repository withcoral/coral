import classNames from 'classnames'
import {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  type MouseEvent,
  type ReactNode,
} from 'react'
import { Link } from 'react-router'

import { Button, Inputs, Menu, ScrollArea, Typography } from '@/wax/components'
import type { IconName } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'

import * as styles from './catalog.css'

export { railItem, toolbarActionLabel } from './catalog.css'

/**
 * A catalog page: a filterable rail of groups on the left and the grouped table
 * of their members on the right. The Tools page is one.
 */
export function CatalogLayout({ rail, children }: { rail: ReactNode; children: ReactNode }) {
  return (
    <div className={styles.layout}>
      {rail}
      <main className={styles.main}>
        <div className={styles.stack}>{children}</div>
      </main>
    </div>
  )
}

export interface FilterFieldProps {
  id?: string
  /** The visible label, accessible name, and placeholder. */
  label: string
  value: string
  onChange: (value: string) => void
  className?: string
}

/** The filter's input; its placeholder names it. */
export function FilterField({ className, ...filter }: FilterFieldProps) {
  return (
    <div className={classNames(styles.filterField, className)}>
      <FilterInput {...filter} />
    </div>
  )
}

function FilterInput({
  id,
  label,
  value,
  onChange,
  ref,
}: FilterFieldProps & { ref?: React.Ref<HTMLInputElement> }) {
  return (
    <Inputs.TextInput
      ref={ref}
      id={id}
      ariaLabel={label}
      value={value}
      onChange={onChange}
      placeholder={label}
      type="search"
    />
  )
}

/** What the rail says when no group matches. */
export interface CatalogRailEmpty {
  message: string
  /** What the reader can do instead, such as add the group they looked for. */
  description?: string
  action?: ReactNode
}

/** The rail: its filter, then one entry per group, or `empty` when none match. */
export function CatalogRail({
  label,
  filter,
  empty,
  children,
}: {
  /** Names the rail and its list for assistive technology. */
  label: string
  filter: FilterFieldProps
  /** Null while a group matches. */
  empty: CatalogRailEmpty | null
  children: ReactNode
}) {
  return (
    <aside className={styles.rail} aria-label={label}>
      <FilterField {...filter} />
      <ScrollArea.Container
        className={styles.railScroll}
        constrainWidth
        fade="none"
        height="auto"
        scrollDirection="vertical"
      >
        <nav className={styles.railList} aria-label={label}>
          {children}
        </nav>
        {/* Mounted while groups match too: a live region that arrives with
            its message is not reliably announced. */}
        <div className={empty ? styles.railEmpty : undefined} role="status">
          {empty && (
            <>
              <Typography.BodyStrong as="p" variant="primary">
                {empty.message}
              </Typography.BodyStrong>
              {empty.description && (
                <Typography.BodySmall as="p" variant="tertiary">
                  {empty.description}
                </Typography.BodySmall>
              )}
              {empty.action && <div className={styles.railEmptyAction}>{empty.action}</div>}
            </>
          )}
        </div>
      </ScrollArea.Container>
    </aside>
  )
}

/** A named run of rail entries, such as the groups a workspace authors itself. */
export function CatalogRailSection({ label, children }: { label: string; children: ReactNode }) {
  const id = useId()
  return (
    <div role="group" aria-labelledby={id} className={styles.railSection}>
      <Typography.BodySmall as="h2" id={id} variant="tertiary" className={styles.railSectionLabel}>
        {label}
      </Typography.BodySmall>
      {children}
    </div>
  )
}

/**
 * One rail entry. A plain click selects it in place; a modified click is the
 * browser's, so a new tab gets the address.
 */
export function CatalogRailItem({
  to,
  current,
  onSelect,
  leading,
  name,
  count,
  trailing,
}: {
  to: string
  current: boolean
  onSelect: () => void
  leading?: ReactNode
  name: string
  /** A count beside the name. */
  count?: number
  /**
   * Controls at the entry's end. They show on the current entry and while an
   * entry has the pointer or keyboard focus.
   */
  trailing?: ReactNode
}) {
  return (
    <div className={styles.railItem} data-current={current ? 'page' : undefined}>
      <Button.Container
        as={Link}
        aria-current={current ? 'page' : undefined}
        className={styles.railLink}
        onClick={(event: MouseEvent<HTMLAnchorElement>) => {
          if (event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return
          event.preventDefault()
          onSelect()
        }}
        to={to}
        replace
        preventScrollReset
        variant="bare"
      >
        {leading}
        <span className={styles.railName}>{name}</span>
        {count !== undefined && <span className={styles.railCount}>{count}</span>}
      </Button.Container>
      {trailing && <span className={styles.railTrailing}>{trailing}</span>}
    </div>
  )
}

// Room the filter's input keeps beside a picker before the picker moves down,
// and the row's gap between items.
const FILTER_ROOM = 200
const TOOLBAR_GAP = 12

/**
 * Whether a picker fits on the title's line beside the filter's input and the
 * action, measured as the toolbar resizes. Its width is measured from its
 * content, so moving it below does not change the answer.
 */
function usePickerPlacement(enabled: boolean) {
  const toolbar = useRef<HTMLDivElement>(null)
  const titleItem = useRef<HTMLDivElement>(null)
  const pickerItem = useRef<HTMLDivElement>(null)
  const actionItem = useRef<HTMLDivElement>(null)
  const [pickerBelow, setPickerBelow] = useState(false)
  const place = useCallback(() => {
    const row = toolbar.current
    const picker = pickerItem.current
    if (!row || !picker) return
    const needed =
      (titleItem.current?.scrollWidth ?? 0) +
      (picker.firstElementChild?.scrollWidth ?? picker.scrollWidth) +
      FILTER_ROOM +
      (actionItem.current?.offsetWidth ?? 0) +
      3 * TOOLBAR_GAP
    setPickerBelow(needed > row.clientWidth)
  }, [])
  useEffect(() => {
    const row = toolbar.current
    if (!enabled || !row || typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(place)
    observer.observe(row)
    return () => observer.disconnect()
  }, [enabled, place])
  // The picker's own width changes with what it shows, such as a count that gains
  // a digit, without the toolbar resizing, so each render measures it again. An
  // unchanged answer does not render again.
  useEffect(() => {
    if (enabled) place()
  })
  return { toolbar, titleItem, pickerItem, actionItem, pickerBelow }
}

/**
 * The one row above the table: `leading` controls at the start, then any
 * other controls, a `picker`, the filter's input, and the page's `action` at
 * the end.
 */
export function CatalogToolbar({
  title,
  titleInfo,
  filter,
  leading,
  picker,
  children,
  action,
}: {
  /**
   * The page's h1, at the start of the row. Tell `PageLayout` the page is
   * `titled`, so its breadcrumb is not a second h1.
   */
  title?: string
  /** An info tip beside the title, such as what a member can see. */
  titleInfo?: ReactNode
  filter: FilterFieldProps & {
    /** A shortcut, such as "$mod+f", that focuses the filter from anywhere on the page. */
    shortcut?: string
    /** Takes the rest of the row, for a toolbar in a narrow column such as a card. */
    fill?: boolean
  }
  /** Controls at the start of the row, such as actions on the selection. */
  leading?: ReactNode
  /**
   * A wide control beside the filter's input, such as a segmented filter. When
   * the row has no room for it, it takes a line of its own below, at the end,
   * and the rest stays on the title's line.
   */
  picker?: ReactNode
  children?: ReactNode
  /**
   * The page's main action, such as "Add provider", last in the row. Give it
   * an icon and put `toolbarActionLabel` on its label: a narrow toolbar shows
   * only the icon.
   */
  action?: ReactNode
}) {
  const { shortcut, fill, ...field } = filter
  const input = useRef<HTMLInputElement>(null)
  const { toolbar, titleItem, pickerItem, actionItem, pickerBelow } = usePickerPlacement(
    Boolean(picker),
  )
  const focusFilter = useCallback((event: KeyboardEvent) => {
    event.preventDefault()
    input.current?.focus()
    input.current?.select()
  }, [])
  const filterField = (
    <div
      className={classNames(
        styles.toolbarFilter,
        fill && styles.toolbarFilterFill,
        field.className,
      )}
    >
      <FilterInput {...field} ref={input} />
      {shortcut && <KeyboardShortcut shortcut={shortcut} handler={focusFilter} />}
    </div>
  )
  const pickerSlot = picker && (
    <div
      ref={pickerItem}
      className={classNames(styles.toolbarPicker, pickerBelow && styles.toolbarPickerBelow)}
    >
      {picker}
    </div>
  )
  return (
    <div
      ref={toolbar}
      className={classNames(styles.toolbar, Boolean(action) && styles.toolbarWithAction)}
    >
      {/* An empty group would still take a gap, and push the controls off the table's edge. */}
      {(title || leading) && (
        <div className={styles.toolbarGroup}>
          {title && (
            <div ref={titleItem} className={styles.toolbarTitle}>
              <Typography.HeadingLarge as="h1" truncate>
                {title}
              </Typography.HeadingLarge>
              {titleInfo}
            </div>
          )}
          {leading}
        </div>
      )}
      <div className={classNames(styles.toolbarGroup, styles.toolbarControls)}>
        {children}
        {!pickerBelow && pickerSlot}
        {action ? (
          // One unit, so a wrapping row never leaves the action on a line alone.
          <div
            className={classNames(styles.toolbarFilterAndAction, fill && styles.toolbarFilterFill)}
          >
            {filterField}
            <div ref={actionItem} className={styles.toolbarAction}>
              {action}
            </div>
          </div>
        ) : (
          filterField
        )}
      </div>
      {pickerBelow && pickerSlot}
    </div>
  )
}

export interface CatalogFilterOption<T extends string> {
  value: T
  label: string
  icon: IconName
}

/** The menu's choices. The first shows every row, so there is always one. */
export type CatalogFilterOptions<T extends string> = readonly [
  CatalogFilterOption<T>,
  ...CatalogFilterOption<T>[],
]

/**
 * A menu beside the filter that narrows the table to one value, such as a
 * status. The first option shows every row; the button names the choice.
 */
export function CatalogFilterMenu<T extends string>({
  ariaLabel,
  options,
  value,
  onChange,
}: {
  ariaLabel: string
  options: CatalogFilterOptions<T>
  value: T
  onChange: (value: T) => void
}) {
  const active = options.find((option) => option.value === value) ?? options[0]
  return (
    <Menu.Container>
      <Menu.Trigger
        render={
          // The name keeps the visible choice, so a screen reader hears it too (WCAG 2.5.3).
          <Button.Container variant="secondary" ariaLabel={`${ariaLabel}: ${active.label}`} />
        }
      >
        <Button.Icon name={active.icon} />
        <Button.Text>{active.label}</Button.Text>
        <Button.Icon name="ChevronDown" />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.RadioGroup value={value} onValueChange={(next: string) => onChange(next as T)}>
          {options.map((option) => (
            <Menu.RadioItem key={option.value} value={option.value} iconName={option.icon}>
              {option.label}
            </Menu.RadioItem>
          ))}
        </Menu.RadioGroup>
      </Menu.Content>
    </Menu.Container>
  )
}
