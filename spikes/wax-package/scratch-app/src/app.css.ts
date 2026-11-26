import { style } from '@vanilla-extract/css'

import { theme } from '@withcoral/wax/theme/theme.css'

// The app's own style reads a Wax token, as Reef's views do.
export const page = style({
  backgroundColor: theme.surface.mainContent,
  padding: 24,
})
