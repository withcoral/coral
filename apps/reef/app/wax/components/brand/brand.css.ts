import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// Sized as withcoral.com sets the lockup: the wordmark two thirds of the mark's height.
export const brand = style({
  alignItems: 'center',
  color: theme.content.primary,
  display: 'inline-flex',
  flexShrink: 0,
  gap: 8,
})

export const mark = style({ display: 'block', flexShrink: 0, height: 28, width: 'auto' })

export const wordmark = style({ display: 'block', flexShrink: 0, height: 18, width: 'auto' })
