import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const panel = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  padding: 16,
})

// The empty outer columns keep the code centred while the copy button sits beside it.
export const row = style({
  alignItems: 'center',
  display: 'grid',
  gap: 12,
  gridTemplateColumns: '1fr auto 1fr',
})

export const code = style({
  gridColumn: 2,
  letterSpacing: '0.08em',
  lineHeight: '32px',
})
