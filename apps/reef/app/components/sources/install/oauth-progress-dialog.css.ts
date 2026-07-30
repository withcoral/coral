import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const code = style({
  letterSpacing: '0.08em',
  lineHeight: '32px',
})

export const codePanel = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  padding: 16,
})

export const error = style({
  color: theme.content.error,
})

export const status = style({
  alignItems: 'center',
  display: 'flex',
  gap: 10,
})
