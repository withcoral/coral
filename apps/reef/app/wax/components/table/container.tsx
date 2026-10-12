import classNames from 'classnames'
import {
  useImperativeHandle,
  useRef,
  type CSSProperties,
  type PropsWithChildren,
  type ReactElement,
  type ReactNode,
  type Ref,
} from 'react'
import { Container as ScrollArea } from '@/wax/components/scroll-area'

import { usePinnedGroup } from './pinned-group'
import { ReadingPosition } from './reading-position'

import type { Column } from './columns'
import { columnStyle } from './columns'
import { ColumnsProvider } from './columns-context'
import type { TableDensity, TableGroupHeadings, TableVariant } from './constants'
import {
  GROUP_HEADING_HEIGHT_PROPERTY,
  groupHeadingHeightPx,
  HEADING_HEIGHT_PX,
  ROWS_MAX_HEIGHT_PROPERTY,
} from './constants'
import * as styles from './table.css'

interface ContainerBaseProps {
  ariaLabel?: string
  children?: ReactNode
  className?: string
  /**
   * One descriptor per column, in order. They set the width of each column and
   * the alignment of everything in it, and `Table.Head` renders their labels.
   */
  columns: readonly Column[]
  density?: TableDensity
  /**
   * The rows do nothing when clicked, so they take no hover colour. Use it when
   * each row's own controls are the only things to act on.
   */
  staticRows?: boolean
  /**
   * Prominent group headings stand taller than the rows and are tinted by
   * level: a top-level heading on the deeper tint, a nested one on the lighter.
   */
  groupHeadings?: TableGroupHeadings
  ref?: Ref<HTMLDivElement>
  variant?: TableVariant
}

interface ContainerAutoLayoutProps extends ContainerBaseProps {
  /** Sizes the columns to their content and scrolls sideways. The default. */
  layout?: 'auto'
  /** The sideways scroll leaves no room for a vertical one. Needs `layout="fixed"`. */
  maxHeight?: never
  /** Nothing scrolls vertically, so no group heading ever pins. Needs `layout="scroll"`. */
  onPinnedGroupChange?: never
}

interface ContainerFixedLayoutProps extends ContainerBaseProps {
  /** Shares the width between the columns, so the table owns no scroll port. */
  layout: 'fixed'
  /** Caps the table height in pixels and scrolls only the rows beneath the heading. */
  maxHeight?: number
  /** The rows scroll inside `Table.Body`, or an ancestor does; the table never does. */
  onPinnedGroupChange?: never
}

interface ContainerScrollLayoutProps extends ContainerBaseProps {
  /** Bordered, two-axis scroll port with sticky headings. The flex parent bounds its height. */
  layout: 'scroll'
  maxHeight?: never
  /**
   * Reports the `id` of the top-level `Table.Group` whose heading sits pinned
   * under the head, or null before the first one, whenever it changes.
   */
  onPinnedGroupChange?: (id: string | null) => void
  /** Keep visible record keys fixed during live updates and restore bounded navigation history. */
  readingKey?: string
  /** No formerly visible record survives; the table retains the nearest valid scroll offset. */
  onReadingPositionLost?: () => void
  /** Temporarily concealed content must not erase identity-only coordinates. */
  readingPaused?: boolean
  /** Follow insertions only at the global edge selected by the caller, never a local page top. */
  readingFollowStart?: boolean
  onReadingStartChange?: (atStart: boolean) => void
  /** Reveal an explicitly selected record once, without moving it on each update. */
  revealRecord?: string
  /**
   * Reports the `recordKey`s on screen, first to last, whenever they change. A
   * pinned heading counts while any row of its group is on screen. Needs `readingKey`.
   */
  onVisibleRecordsChange?: (keys: string[]) => void
}

export type ContainerProps = PropsWithChildren<
  | ((ContainerAutoLayoutProps | ContainerFixedLayoutProps) & {
      readingKey?: never
      onReadingPositionLost?: never
      readingPaused?: never
      readingFollowStart?: never
      onReadingStartChange?: never
      revealRecord?: never
      onVisibleRecordsChange?: never
    })
  | ContainerScrollLayoutProps
>

export function Container({
  ariaLabel,
  children,
  className,
  columns,
  density = 'default',
  groupHeadings = 'plain',
  layout = 'auto',
  maxHeight,
  onPinnedGroupChange,
  ref,
  readingKey,
  onReadingPositionLost,
  readingPaused,
  readingFollowStart,
  onReadingStartChange,
  revealRecord,
  onVisibleRecordsChange,
  staticRows = false,
  variant = 'plain',
}: ContainerProps) {
  const readingRef = useRef<HTMLDivElement>(null)
  useImperativeHandle(ref, () => readingRef.current!)
  const groupHeadingHeight = groupHeadingHeightPx(density, groupHeadings)
  const onScroll = usePinnedGroup(readingRef, density, groupHeadingHeight, onPinnedGroupChange)
  if (
    maxHeight !== undefined &&
    (!Number.isFinite(maxHeight) || maxHeight < HEADING_HEIGHT_PX[density])
  ) {
    throw new RangeError(
      `Table maxHeight must be a finite number at least as tall as its ${HEADING_HEIGHT_PX[density]}px heading.`,
    )
  }

  const bounds =
    maxHeight === undefined
      ? undefined
      : ({
          [ROWS_MAX_HEIGHT_PROPERTY]: `${maxHeight - HEADING_HEIGHT_PX[density]}px`,
        } as CSSProperties)
  return (
    <ReadingPosition
      element={readingRef}
      options={{
        key: readingKey,
        onLost: onReadingPositionLost,
        paused: readingPaused,
        followStart: readingFollowStart,
        onStartChange: onReadingStartChange,
        reveal: revealRecord,
        onVisible: onVisibleRecordsChange,
      }}
    >
      {wrapScroll(
        layout,
        <div
          aria-label={ariaLabel}
          className={classNames(
            styles.container({ density, layout, variant }),
            variant === 'card' && styles.cardTable,
            layout === 'scroll' && styles.scrollTable,
            readingKey && styles.anchoredTable,
            maxHeight !== undefined && styles.scrollRows,
            groupHeadings === 'prominent' && styles.prominentGroupHeadings,
            staticRows && styles.staticRows,
            className,
          )}
          ref={readingRef}
          role="table"
          onScroll={onScroll}
          style={
            {
              ...columnStyle(columns, layout),
              ...bounds,
              [GROUP_HEADING_HEIGHT_PROPERTY]: `${groupHeadingHeight}px`,
            } as CSSProperties
          }
        >
          <ColumnsProvider columns={columns} layout={layout}>
            {children}
          </ColumnsProvider>
        </div>,
      )}
    </ReadingPosition>
  )
}

/**
 * A scroll table stays its own scroll port, so sticky headings, pinned groups,
 * and virtualizers keep measuring it. The scroll area only draws its scrollbars.
 */
function wrapScroll(layout: ContainerProps['layout'], table: ReactElement) {
  if (layout !== 'scroll') return table
  return (
    <ScrollArea
      className={styles.scrollArea}
      fade="none"
      height="auto"
      horizontal
      renderViewport={table}
    />
  )
}
