import { style } from '../../../css'

import { theme } from '../../../theme/theme.css'

export const invalid = style({
  borderColor: theme.input.stroke.error,
  selectors: {
    '&:hover:not(:focus-within)': {
      borderColor: theme.input.stroke.error,
    },
  },
})
