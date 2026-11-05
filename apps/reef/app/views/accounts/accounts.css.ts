import { createContainer, globalStyle, style } from '@vanilla-extract/css'

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
const rowActionsContainer = createContainer()
// A request's deadline, Decline, then Connect, on one line at the end of a row.
export const rowActions = style({
  containerName: rowActionsContainer,
  containerType: 'inline-size',
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  justifyContent: 'flex-end',
  whiteSpace: 'nowrap',
  paddingBlock: 6,
})
globalStyle(`${rowActions} > *`, { flexShrink: 0 })
// A request's deadline: it truncates first, so the buttons stay whole.
// A row too narrow for the short deadline shows only the buttons.
export const requestNote = style({
  selectors: { [`${rowActions} > &`]: { flexShrink: 1 } },
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  '@container': { [`${rowActionsContainer} (max-width: 279.98px)`]: { display: 'none' } },
})
// The full deadline fits beside both buttons from 392px; a narrower row shows the short one.
export const wideNote = style({
  display: 'none',
  '@container': { [`${rowActionsContainer} (min-width: 392px)`]: { display: 'inline' } },
})
export const narrowNote = style({
  '@container': { [`${rowActionsContainer} (min-width: 392px)`]: { display: 'none' } },
})
// The provider's logo, then its name.
export const provider = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minWidth: 0,
})
