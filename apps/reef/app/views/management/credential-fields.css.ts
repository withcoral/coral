import { style } from '@vanilla-extract/css'

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

/**
 * A reference in one cell: its store, the path, then the key when the store takes
 * one. They sit as far apart as the controls of two cells: each pads 4px.
 */
const REFERENCE_GAP = 8
export const referenceInputs = style({
  alignItems: 'center',
  display: 'flex',
  gap: REFERENCE_GAP,
  minWidth: 0,
  width: '100%',
})
/** The store keeps its longest name's width, so a new store does not move the path. */
export const referenceStore = style({ flex: 'none' })
export const referenceParts = style({ display: 'flex', flex: 1, gap: REFERENCE_GAP, minWidth: 0 })
export const referencePath = style({ flex: 2, minWidth: 0 })
export const referenceKey = style({ flex: 1, minWidth: 0 })
