import { style } from '@vanilla-extract/css'

export const page = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  marginInline: 'auto',
  maxWidth: 720,
  padding: '32px 24px',
  width: '100%',
})
export const stack = style({ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' })
