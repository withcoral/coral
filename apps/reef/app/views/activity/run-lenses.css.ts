import { style } from '@vanilla-extract/css'

/** A figure, then its bar against the step that moved the most. */
export const meterCell = style({ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 })
export const figure = style({ minWidth: 56, fontVariantNumeric: 'tabular-nums' })
export const meter = style({ flex: 1, minWidth: 24 })
export const note = style({ padding: 16 })
