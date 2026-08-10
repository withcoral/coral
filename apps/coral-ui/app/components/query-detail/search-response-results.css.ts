import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
  overflowY: 'auto',
  paddingInlineEnd: 4,
})

export const resultTable = style({
  flexShrink: 0,
  maxWidth: '100%',
  minWidth: 0,
})

export const disclosureCell = style({ justifyContent: 'center' })

export const disclosureSpacer = style({ height: 22, width: 22 })

export const providerBadges = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 6,
  minWidth: 0,
})

export const resultDetailRow = style({ backgroundColor: theme.surface.onMainContentSubtle })

export const resultDetailCell = style({ minWidth: 0 })

export const resultBody = style({
  display: 'grid',
  gap: 16,
  minWidth: 0,
  paddingBlock: 8,
  width: '100%',
})

export const section = style({ display: 'grid', gap: 6, minWidth: 0 })

export const sectionTitle = style({ margin: 0 })

export const bodyCopy = style({ margin: 0 })

export const requiredStar = style({
  color: theme.content.error,
  cursor: 'help',
  display: 'inline-flex',
  flexShrink: 0,
  font: 'inherit',
  marginInlineStart: 4,
  outlineOffset: 2,
})

export const matchingValues = style({
  display: 'grid',
  gap: 6,
  margin: 0,
})

export const matchingValueRow = style({
  alignItems: 'baseline',
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'max-content minmax(0, 1fr)',
  minWidth: 0,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: 4,
      gridTemplateColumns: 'minmax(0, 1fr)',
    },
  },
})

export const matchingValueField = style({
  maxWidth: 'min(320px, 40vw)',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const matchingValueCopy = style({
  margin: 0,
  minWidth: 0,
  overflowWrap: 'anywhere',
})

export const resultState = style({
  alignItems: 'center',
  border: `1px dashed ${theme.stroke.primary}`,
  borderRadius: 8,
  display: 'flex',
  justifyContent: 'center',
  minHeight: 140,
  padding: 24,
  textAlign: 'center',
})
