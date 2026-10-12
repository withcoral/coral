import { style } from '@/wax/css'

export const brand = style({
  alignItems: 'center',
  display: 'inline-flex',
  flexShrink: 0,
  gap: '10px',
})

// The mark carries the weight; the wordmark sits beside it at regular weight.
export const wordmark = style({ fontWeight: 400, letterSpacing: '-0.01em', lineHeight: 1 })
