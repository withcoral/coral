import type { CSSProperties } from 'react'

import type { TableAlign } from './constants'
import { CELL_ALIGN_PROPERTY, CELL_JUSTIFY_PROPERTY } from './constants'

const JUSTIFY: Record<TableAlign, string> = {
  center: 'center',
  left: 'flex-start',
  right: 'flex-end',
}

/** A single cell's alignment, ahead of its column's in the `var()` chain. */
export function alignOverride(align?: TableAlign): CSSProperties | undefined {
  if (align === undefined) return undefined
  return { [CELL_ALIGN_PROPERTY]: align, [CELL_JUSTIFY_PROPERTY]: JUSTIFY[align] } as CSSProperties
}
