import { style } from '@vanilla-extract/css'

import { baseInput } from '@/wax/components/inputs/base-input.css'
import { theme } from '@/wax/theme/theme.css'

export const container = style([
  baseInput,
  {
    alignItems: 'center',
    display: 'flex',
    gap: '8px',
    height: '32px',
    minWidth: 0,
    paddingInlineEnd: '4px',
    selectors: {
      '&:focus-within': {
        borderColor: theme.input.stroke.focus,
      },
      '&:hover:not(:focus-within)': {
        borderColor: theme.input.stroke.hover,
      },
    },
  },
])

export const content = style({
  flex: 1,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
