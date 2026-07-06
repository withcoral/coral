import classNames from 'classnames'
import { CSSProperties } from 'react'

import * as styles from './skeleton.css'

export function Skeleton({
  borderRadius,
  className,
  height = '20px',
  style,
  width = '80px',
}: {
  borderRadius?: number | string
  className?: string
  height?: number | string
  style?: CSSProperties
  width?: number | string
}) {
  const inlineStyles: CSSProperties = {
    borderRadius,
    height: typeof height === 'number' ? `${height}px` : height,
    width: typeof width === 'number' ? `${width}px` : width,
    ...style,
  }

  return <div className={classNames(styles.container, className)} style={inlineStyles} />
}
