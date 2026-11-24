import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

export const list = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
})
export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
})

/** A field's name in its row, with its description under it. */
export const fieldName = style({ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 })

/** A reference in one cell: the path, then the key when the store takes one. */
export const referenceInputs = style({ display: 'flex', minWidth: 0, width: '100%' })
export const referencePath = style({ flex: 2, minWidth: 0 })
export const referenceKey = style({
  borderInlineStart: `1px solid ${theme.stroke.floating}`,
  flex: 1,
  minWidth: 0,
})
