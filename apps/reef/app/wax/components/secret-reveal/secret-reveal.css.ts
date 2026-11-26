import { style } from '@/wax/css'

export const root = style({ gap: 16, minWidth: 0 })
export const intro = style({ display: 'flex', flexDirection: 'column', gap: 8 })
export const secret = style({
  overflowWrap: 'anywhere',
  padding: 16,
  userSelect: 'all',
  whiteSpace: 'pre-wrap',
})
export const actions = style({ alignItems: 'center', display: 'flex', flexWrap: 'wrap', gap: 12 })
