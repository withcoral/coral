import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// Keeps the surrounding text's size, so a match reads the same in any cell.
// The heaviest weight: one step up from the text is too faint to find a match.
export const match = style({
  color: theme.content.primary,
  fontWeight: theme.typography.bodySmallXStrong.fontWeight,
})

// A find's match is also tinted, so it shows in code; the current one is outlined too.
export const found = style({
  background: theme.content.warningBackground,
  borderRadius: 2,
  color: 'inherit',
})
export const current = style({
  outline: `2px solid ${theme.stroke.focused}`,
  outlineOffset: 1,
})
