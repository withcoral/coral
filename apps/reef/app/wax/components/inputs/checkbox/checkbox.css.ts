import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const label = style({
  alignItems: 'center',
  color: theme.content.primary,
  cursor: 'pointer',
  display: 'inline-flex',
  gap: 8,
  ...theme.typography.body,
  selectors: { '&:has(:disabled)': { color: theme.content.disabled, cursor: 'not-allowed' } },
})
export const input = style({
  accentColor: theme.stroke.focused,
  cursor: 'inherit',
  height: 16,
  margin: 0,
  width: 16,
  selectors: {
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: 2 },
  },
})
