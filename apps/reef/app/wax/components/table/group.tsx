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
 * per top-level group. Heading rows carry aria-level, but the hierarchy is not a
 * treegrid: groups do not expand, collapse, or add keyboard hierarchy.
 */
export function Group({
  id,
  index,
  label,
  heading,
  headingRow,
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
  headingRow?: Pick<RowProps, 'recordKey' | 'selected' | 'status' | 'onClick'>
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
      <GroupDepthContext value={depth + 1}>{children}</GroupDepthContext>
    </div>
  )
}
