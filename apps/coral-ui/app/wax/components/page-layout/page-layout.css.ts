import { style } from '@/wax/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const container = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  height: '100%',
  overflow: 'hidden',
})

export const topBar = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: '12px',
      paddingInline: '16px',
    },
  },
  alignItems: 'center',
  borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  gap: '24px',
  justifyContent: 'space-between',
  paddingBlock: '6px',
  paddingInline: '32px',
})

export const topBarActions = style({
  alignItems: 'center',
  display: 'flex',
  gap: '4px',
})

export const content = style({
  flex: 1,
  overflow: 'hidden',
})
