import { style, styleVariants } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

/** The logo and the name on one line, which gives way to the name's ellipsis. */
export const root = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  maxWidth: '100%',
  minWidth: 0,
  verticalAlign: 'middle',
})

export const logo = style({ display: 'inline-flex', flexShrink: 0 })

// The colour is its context's: a tool reads as one more value in its row.
const name = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

/** One face for every tool name: the body font, never code, in a table or a line of
 * metadata alike. */
export const names = styleVariants({
  default: [name, theme.typography.body],
  small: [name, theme.typography.bodySmall],
})
