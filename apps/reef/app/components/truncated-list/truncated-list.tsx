import classNames from 'classnames'
import * as React from 'react'
import TruncateMarkup from 'react-truncate-markup'

import { Tooltip } from '@/wax/components/tooltip'

import * as styles from './truncated-list.css'

interface TruncatedListProps<T> {
  /** Additional class name for the flex container */
  containerClassName?: string
  /** Function to get a unique key for each item */
  getKey: (item: T) => string
  /** The items to render */
  items: T[]
  /** Number of lines to show before truncating */
  linesClamp?: number
  /** Render each item */
  renderItem: (item: T) => React.ReactNode
  /** Render the overflow content shown in the tooltip */
  renderOverflowContent: (hiddenItems: T[]) => React.ReactNode
  /** Render the overflow trigger (e.g., "+3" chip) */
  renderOverflowTrigger: (hiddenCount: number) => React.ReactNode
  /** Additional class name for the tooltip popup */
  tooltipClassName?: string
}

/**
 * Renders a list of items that truncates after a specified number of lines with a "+N" overflow indicator.
 * Hidden items are shown in a tooltip when hovering over the overflow indicator.
 */
export function TruncatedList<T>({
  containerClassName,
  getKey,
  items,
  linesClamp = 1,
  renderItem,
  renderOverflowContent,
  renderOverflowTrigger,
  tooltipClassName,
}: TruncatedListProps<T>) {
  return (
    <TruncateMarkup
      // oxlint-disable-next-line react/no-unstable-nested-components -- TruncateMarkup expects an ellipsis render callback.
      ellipsis={(node: React.ReactNode) => {
        if (!React.isValidElement<{ children: React.ReactNode }>(node)) {
          return null
        }

        const renderedCount = React.Children.count(node.props.children)
        const hiddenCount = items.length - renderedCount
        const hiddenItems = items.slice(renderedCount)

        return (
          <Tooltip
            className={tooltipClassName}
            content={
              <div className={styles.tooltipContent} onClick={(e) => e.stopPropagation()}>
                {renderOverflowContent(hiddenItems)}
              </div>
            }
            sideOffset={4}
          >
            <span className={styles.overflowTrigger}>{renderOverflowTrigger(hiddenCount)}</span>
          </Tooltip>
        )
      }}
      lines={linesClamp}
    >
      {/* Need the inline style here to override TruncateMarkup, which has `display: (node.props.style || {}).display || 'block'`` */}
      <div className={classNames(styles.container, containerClassName)} style={{ display: 'flex' }}>
        {items.map((item) => (
          <TruncateMarkup.Atom key={getKey(item)}>{renderItem(item)}</TruncateMarkup.Atom>
        ))}
      </div>
    </TruncateMarkup>
  )
}
