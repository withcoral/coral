import classNames from 'classnames'
import { CSSProperties } from 'react'

import type { TypographyVariant } from '../typography'
import { typography } from '../typography/typography.css'

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

/**
 * A placeholder one line of `variant` text tall, so the line that replaces it does not
 * move what follows. The bar is as tall as the text's letters.
 */
export function SkeletonText({
  variant,
  width = '80px',
}: {
  variant: TypographyVariant
  width?: number | string
}) {
  return (
    <span className={classNames(typography({ variant }), styles.textLine)} style={{ width }}>
      <span className={classNames(styles.container, styles.textBar)} />
    </span>
  )
}
