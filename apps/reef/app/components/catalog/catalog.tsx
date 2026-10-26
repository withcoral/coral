import classNames from 'classnames'
import { useCallback, useRef, type MouseEvent, type ReactNode } from 'react'
import { Link } from 'react-router'

import { Button, Inputs, Menu, ScrollArea, Typography } from '@/wax/components'
import type { IconName } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'

import * as styles from './catalog.css'

export { railItem, toolbarActionLabel } from './catalog.css'

/**
 * A catalog page: a filterable rail of groups on the left and the grouped table
 * of their members on the right. Providers and Functions share it.
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

/**
 * The rail: its filter, then one entry per group, or `emptyMessage` when none
 * match.
 */
export function CatalogRail({
  label,
  filter,
  empty,
  emptyMessage,
  children,
}: {
  /** Names the rail and its list for assistive technology. */
  label: string
  filter: FilterFieldProps
  empty: boolean
  emptyMessage: string
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
          {empty && <Typography.BodySmall as="p">{emptyMessage}</Typography.BodySmall>}
        </nav>
      </ScrollArea.Container>
    </aside>
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

/**
 * The one row above the table: `leading` controls at the start, then any
 * other controls, the filter's input, and the page's `action` at the end.
 */
export function CatalogToolbar({
  title,
  titleInfo,
  filter,
  leading,
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
  return (
    <div className={classNames(styles.toolbar, Boolean(action) && styles.toolbarWithAction)}>
      {/* An empty group would still take a gap, and push the controls off the table's edge. */}
      {(title || leading) && (
        <div className={styles.toolbarGroup}>
          {title && (
            <div className={styles.toolbarTitle}>
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
        {action ? (
          // One unit, so a wrapping row never leaves the action on a line alone.
          <div
            className={classNames(styles.toolbarFilterAndAction, fill && styles.toolbarFilterFill)}
          >
            {filterField}
            <div className={styles.toolbarAction}>{action}</div>
          </div>
        ) : (
          filterField
        )}
      </div>
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
