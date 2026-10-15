import { style } from '@vanilla-extract/css'

/** The icon plate's height: the heading's first line centres on it. */
const firstLine = 32
/** The heading's line height (BodyLargeStrong). */
const headingLine = 24

export const panel = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })

export const header = style({ alignItems: 'start' })

export const summary = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  minWidth: 0,
  flex: 1,
})

// The first line of a heading, wrapped or not, centres on the icon instead of riding high.
export const title = style({
  overflowWrap: 'anywhere',
  minWidth: 0,
  paddingBlock: (firstLine - headingLine) / 2,
})

// Lines under the heading keep their spacing from the heading text, not from its padding.
export const details = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  marginTop: -(firstLine - headingLine) / 2,
  selectors: { '&:empty': { display: 'none' } },
})

export const trailing = style({
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  flexShrink: 0,
  minHeight: firstLine,
})

// Nothing to decide leaves no empty bar at the drawer's bottom.
export const footerSlot = style({ selectors: { '&:empty': { display: 'none' } } })

// The drawer's own controls sit together, apart from the record's status.
export const railControls = style({
  display: 'flex',
  alignItems: 'center',
  gap: 2,
})
