import { keyframes, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

const spin = keyframes({
  from: { transform: 'rotate(0deg)' },
  to: { transform: 'rotate(360deg)' },
})

export const statePanel = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 12,
  justifyContent: 'center',
  minHeight: 240,
  padding: 24,
  textAlign: 'center',
})

export const stateIcon = style({
  animation: `${spin} 1s linear infinite`,
})

export const resultTable = style({
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
})
