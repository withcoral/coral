import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const page = style({
  alignItems: 'center',
  backgroundColor: theme.surface.main,
  boxSizing: 'border-box',
  display: 'flex',
  minHeight: '100dvh',
  padding: '24px',
})

export const content = style({
  alignItems: 'center',
  display: 'flex',
  flexDirection: 'column',
  gap: '28px',
  margin: '0 auto',
  maxWidth: '360px',
  textAlign: 'center',
  width: '100%',
})

export const panel = style({
  gap: '24px',
  padding: 'clamp(24px, 5vw, 32px)',
})

export const actions = style({
  width: '100%',
})
