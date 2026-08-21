import { style } from '@/wax/css'

import { breakpoints } from '@/styles/theme.css'
import { utils } from '@/styles/utils'
import { theme } from '@/wax/theme/theme.css'

export const container = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: '12px',
    },
  },
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
})

export const item = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: '6px',
      paddingBlock: 12,
      paddingInline: 14,
    },
  },
  alignItems: 'flex-start',
  alignSelf: 'stretch',
  border: '1px solid',
  borderColor: theme.stroke.secondary,
  borderRadius: '12px',
  ...utils.boxClamp(5),
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  paddingBlock: 16,
  paddingInline: 20,

  selectors: {
    '&:hover': {
      backgroundColor: theme.surface.main,
    },
  },
})

export const footer = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexWrap: 'wrap',
      gap: '8px 12px',
    },
  },
  alignItems: 'center',
  alignSelf: 'stretch',
  color: theme.content.tertiary,
  display: 'flex',
  flex: '1 0 0',
  gap: 16,
})

export const interactive = style({
  cursor: 'pointer',
})
