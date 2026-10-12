import { style } from '@/wax/css'

import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

export const list = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
export const row = style({
  alignItems: 'center',
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'grid',
  gap: '4px 24px',
  gridTemplateColumns: 'minmax(0, 1fr) auto',
  paddingBlock: 16,
  selectors: { '&:first-child': { borderTop: 0, paddingTop: 0 } },
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gridTemplateColumns: 'minmax(0, 1fr)' },
  },
})
export const text = style({ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 })
export const label = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 })
export const action = style({ display: 'flex', flexWrap: 'wrap', gap: 8, justifySelf: 'end' })
