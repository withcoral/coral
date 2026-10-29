import { style } from '@/wax/css'

/** A tall viewer: its header, then content that fills the rest and scrolls inside. */
export const popup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  height: 'min(90vh, 1200px)',
})
