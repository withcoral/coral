import classNames from 'classnames'
import type { PropsWithChildren, Ref } from 'react'

import { Container as ScrollArea } from '@/wax/components/scroll-area'

import * as styles from './table.css'

export type BodyProps = PropsWithChildren<{
  className?: string
  ref?: Ref<HTMLDivElement>
}>

export function Body({ children, className, ref }: BodyProps) {
  return (
    <ScrollArea
      className={styles.bodyScrollArea}
      fade="none"
      height="auto"
      renderViewport={
        <div className={classNames(styles.body, className)} role="rowgroup">
          {children}
        </div>
      }
      scrollDirection="vertical"
      viewportRef={ref}
    />
  )
}
