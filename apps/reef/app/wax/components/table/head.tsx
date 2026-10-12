import classNames from 'classnames'
import type { PropsWithChildren, Ref } from 'react'

import { useColumns } from './columns-context'
import { Heading } from './heading'
import { Row } from './row'
import * as styles from './table.css'

export type HeadProps = PropsWithChildren<{
  className?: string
  ref?: Ref<HTMLDivElement>
}>

export function Head({ children, className, ref }: HeadProps) {
  const columns = useColumns()
  return (
    <div className={classNames(styles.head, className)} ref={ref} role="rowgroup">
      {/* The labels its container was given, unless a caller renders its own
          heading row for the cases a label cannot express. */}
      {children ?? (
        <Row>
          {columns.map((column, index) => (
            <Heading ariaLabel={column.ariaLabel} key={index}>
              {column.label}
            </Heading>
          ))}
        </Row>
      )}
    </div>
  )
}
