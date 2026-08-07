import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const page = style({
  alignItems: 'center',
  backgroundColor: theme.surface.mainContent,
  boxSizing: 'border-box',
  display: 'flex',
  minHeight: '100dvh',
  padding: '24px',
})

export const content = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '12px',
  margin: '0 auto',
  maxWidth: '420px',
  textAlign: 'center',
})

export const actions = style({
  display: 'flex',
  justifyContent: 'center',
  paddingTop: '8px',
})
