import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// Sized as withcoral.com sets the lockup: the wordmark two thirds of the mark's height.
export const brand = style({
  alignItems: 'center',
  color: theme.content.primary,
  display: 'inline-flex',
  flexShrink: 0,
  gap: 9,
})

export const mark = style({ display: 'block', flexShrink: 0, height: 22, width: 'auto' })

export const wordmark = style({ display: 'block', flexShrink: 0, height: 15, width: 'auto' })

export const wordmarkRow = style({ alignItems: 'baseline', display: 'flex', gap: 8 })

// No line height, so the label adds no height to the row: the wordmark keeps
// its place beside the mark, and the label hangs on its baseline.
export const label = style({
  ...theme.typography.body,
  color: theme.content.tertiary,
  lineHeight: 0,
  whiteSpace: 'nowrap',
})
