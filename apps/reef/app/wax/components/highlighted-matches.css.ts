import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// Keeps the surrounding text's size, so a match reads the same in any cell.
// The heaviest weight: one step up from the text is too faint to find a match.
export const match = style({
  color: theme.content.primary,
  fontWeight: theme.typography.bodySmallXStrong.fontWeight,
})
