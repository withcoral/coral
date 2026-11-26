import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  minWidth: 0,
  borderTop: `1px solid ${theme.stroke.secondary}`,
  paddingTop: 16,
})
export const summary = style({ cursor: 'pointer', paddingBlock: 8 })
export const rootInline = style({ minWidth: 0 })
export const summaryInline = style({
  color: theme.content.link,
  cursor: 'pointer',
  fontWeight: 500,
  paddingBlock: 2,
})
export const content = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  paddingBlock: 12,
  minWidth: 0,
})
