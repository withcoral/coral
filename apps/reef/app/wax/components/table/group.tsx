import classNames from 'classnames'
import {
  Children,
  cloneElement,
  createContext,
  Fragment,
  isValidElement,
  useContext,
  useId,
  type CSSProperties,
  type ReactElement,
  type ReactNode,
} from 'react'
import * as Button from '@/wax/components/button'
import { Row, type RowProps } from './row'
import { Cell } from './cell'
import { Typography } from '@/wax/components/typography'
import { GROUP_DEPTH_PROPERTY } from './constants'
import * as styles from './table.css'

const GroupDepthContext = createContext(0)

function indentFirstHeadingCell(heading: ReactNode): ReactNode {
  let indented = false

  function indentNode(node: ReactNode): ReactNode {
    if (indented || !isValidElement<{ children?: ReactNode; className?: string }>(node)) {
      return node
    }

    if (node.type === Fragment) {
      return cloneElement(node, undefined, Children.map(node.props.children, indentNode))
    }

    indented = true
    return cloneElement(node as ReactElement<{ className?: string }>, {
      className: classNames(node.props.className, styles.groupHeadingFirstCell),
    })
  }

  return Children.map(heading, indentNode)
}

/**
 * A labelled row group sharing the table's columns and its ancestor's scroll area.
 *
 * Nested groups render presentational wrappers so the table exposes one rowgroup
 * per top-level group. Heading rows carry aria-level. A `collapsed` group keeps
 * its heading and drops its rows; put a `GroupToggle` in the heading to open it
 * again. An `active` heading is the keyboard cursor, as `Row active` is.
 */
export function Group({
  id,
  index,
  label,
  heading,
  headingRow,
  collapsed = false,
  active = false,
  children,
}: {
  /** DOM id of the group element, for callers that scroll to it or measure it. */
  id?: string
  /** The heading's index in a `useVirtualRows` list, so `scrollToIndex` can reach it. */
  index?: number
  label: string
  /** Custom heading cells, pinned below the header in a scroll-layout table. */
  heading?: ReactNode
  /**
   * Record identity and interaction for a heading that is itself a record row.
   * Its cells keep their row layout, so nothing moves when the record expands.
   */
  headingRow?: Pick<RowProps, 'recordKey' | 'selected' | 'active' | 'status' | 'onClick'>
  /** Shows only the heading. A virtualized caller also leaves the rows out of its list. */
  collapsed?: boolean
  /** The heading is the keyboard cursor's row. */
  active?: boolean
  children: ReactNode
}) {
  const labelId = useId()
  const depth = useContext(GroupDepthContext)
  const nested = depth > 0
  return (
    <div
      id={id}
      role={nested ? 'presentation' : 'rowgroup'}
      aria-label={!nested && heading ? label : undefined}
      aria-labelledby={!nested && !heading ? labelId : undefined}
      className={classNames(styles.group, nested && styles.nestedGroup)}
      style={{ [GROUP_DEPTH_PROPERTY]: depth } as CSSProperties}
    >
      <Row
        {...headingRow}
        active={active || headingRow?.active}
        data-pinned-heading
        data-index={index}
        className={styles.groupHeading({ depth: nested ? 'nested' : 'top' })}
        aria-level={depth + 1}
      >
        {heading ? (
          // A record heading shows its own depth; a plain one takes the group indent.
          headingRow ? (
            heading
          ) : (
            indentFirstHeadingCell(heading)
          )
        ) : (
          <Cell fullWidth className={styles.groupHeadingFirstCell}>
            <Typography.BodySmallStrong id={labelId}>{label}</Typography.BodySmallStrong>
          </Cell>
        )}
      </Row>
      {!collapsed && <GroupDepthContext value={depth + 1}>{children}</GroupDepthContext>}
    </div>
  )
}

/**
 * The button that collapses and expands a group, for a custom heading. The
 * chevron's direction shows the state, so it does not rest on colour.
 */
export function GroupToggle({
  collapsed,
  label,
  onToggle,
}: {
  collapsed: boolean
  /** The group's name, which the button's accessible name includes. */
  label: string
  onToggle: () => void
}) {
  return (
    <Button.Container
      variant="bare"
      size="22"
      ariaLabel={`${collapsed ? 'Expand' : 'Collapse'} ${label}`}
      aria-expanded={!collapsed}
      onClick={(event) => {
        // The toggle is its own control: a heading row's click does not follow it.
        event.stopPropagation()
        onToggle()
      }}
    >
      <Button.Icon name={collapsed ? 'ChevronRight' : 'ChevronDown'} />
    </Button.Container>
  )
}
