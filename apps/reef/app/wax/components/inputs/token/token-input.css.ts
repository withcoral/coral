import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const invalid = style({
  borderColor: theme.input.stroke.error,
  selectors: {
    '&:hover:not(:focus-within)': {
      borderColor: theme.input.stroke.error,
    },
  },
})
