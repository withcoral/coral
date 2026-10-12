import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '1rem',
  alignItems: 'flex-start',
})
export const actions = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: '0.5rem',
  alignItems: 'center',
})
export const account = style({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'flex-start',
  gap: '0.75rem',
  paddingBlock: '1rem',
})
export const reference = style({ overflowWrap: 'anywhere' })
export const permissions = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '0.75rem',
  minWidth: 0,
})
export const missing = style({ color: theme.content.error })
// The same details card as a provider's Source panel in its settings.
export const details = style({ gap: 16, minWidth: 0 })
export const facts = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  margin: 0,
  minWidth: 0,
})
export const fact = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  margin: 0,
  minWidth: 0,
})
export const factValue = style({ margin: 0, minWidth: 0 })
// The page heading with what to do next, and the action that does it at the right.
export const heading = style({
  alignItems: 'flex-start',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 16,
  justifyContent: 'space-between',
})
export const headingText = style({
  display: 'flex',
  flex: '1 1 240px',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})
// One row: the inputs take the room, and the submit button keeps its width at the end.
export const credentialForm = style({
  alignItems: 'flex-end',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '0.75rem',
})
export const credentialInputs = style({ flex: '1 1 280px', minWidth: 0 })
// The state, then a waiting connection request beneath it.
export const accountStatus = style({
  alignItems: 'flex-start',
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  paddingBlock: 8,
})
