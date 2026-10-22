import { style } from '@/wax/css'

import { breakpoints } from '@/styles/breakpoints'

export const section = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
export const title = style({ paddingBottom: 8 })
export const list = style({ display: 'flex', flexDirection: 'column', margin: 0, minWidth: 0 })
export const row = style({
  alignItems: 'start',
  display: 'grid',
  gap: '4px 16px',
  gridTemplateColumns: 'minmax(96px, 1fr) minmax(0, 3fr)',
  paddingBlock: 8,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gridTemplateColumns: 'minmax(0, 1fr)' },
  },
})
export const label = style({ overflowWrap: 'anywhere' })
export const value = style({
  margin: 0,
  minWidth: 0,
  overflowWrap: 'anywhere',
  whiteSpace: 'pre-wrap',
})
