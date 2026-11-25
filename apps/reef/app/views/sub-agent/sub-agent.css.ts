import { style } from '@vanilla-extract/css'

export const form = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  maxWidth: 640,
  minWidth: 0,
  paddingTop: 8,
})

export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
})

// The menu button sizes to its label, not to the field.
export const picker = style({ display: 'flex' })

// A saved value keeps its lines and breaks anywhere, as a URL or JSON needs.
export const savedNow = style({ overflowWrap: 'anywhere', whiteSpace: 'pre-wrap' })

// The state of the saved key, and the buttons that change it.
export const saved = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 })

export const advanced = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  paddingTop: 8,
})
