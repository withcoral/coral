import { recipe, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

import { breakpoints } from '@/styles/breakpoints'

export const container = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minWidth: 0,
  overflow: 'hidden',
})

export const topBar = style({
  alignItems: 'center',
  borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexShrink: 0,
  gap: 24,
  justifyContent: 'space-between',
  // Keep single-segment and linked breadcrumbs at the same height: one row of links.
  minHeight: 44,
  paddingInline: 24,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gap: 12, paddingInline: 16 },
  },
})

export const topBarActions = style({ alignItems: 'center', display: 'flex', gap: 4 })
export const scrollContainer = style({
  flex: '1 1 auto',
  minHeight: 0,
  minWidth: 0,
})

export const frame = recipe({
  base: {
    display: 'flex',
    flexDirection: 'column',
    gap: 32,
    minWidth: 0,
    padding: '32px 24px',
    width: '100%',
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: { padding: '24px 16px', gap: 24 },
    },
  },
  variants: {
    variant: {
      standard: { marginInline: 'auto', maxWidth: 960, minHeight: '100%' },
      wide: { minHeight: '100%' },
      // A split page's panes start close under the top bar and end as close
      // to the bottom, so their tables get the height.
      split: { height: '100%', minHeight: 0, paddingBlock: 16 },
    },
  },
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  gap: 12,
  minWidth: 0,
  // Conditional notices can all render nothing; the frame's gap must not remain.
  selectors: { '&:empty': { display: 'none' } },
})

export const splitBody = style({ flex: '1 1 auto', minHeight: 0, minWidth: 0 })
