import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const box = style({
  alignItems: 'flex-start',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  gap: 10,
  padding: 12,
})
