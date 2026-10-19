import { recipe, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

import { breakpoints } from '@/styles/breakpoints'

import { TOP_BAR_HEIGHT_PX } from './constants'

// A breadcrumb link's height: the one row the bar holds.
const LINK_HEIGHT_PX = 32

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
  // Level with the sidebar's masthead beside it: the breadcrumbs centre on the
  // brand, with the same room under them as above. The bar stays one row of
  // links tall, so single-segment and linked breadcrumbs match.
  minHeight: TOP_BAR_HEIGHT_PX,
  paddingBlock: (TOP_BAR_HEIGHT_PX - LINK_HEIGHT_PX) / 2,
  paddingInline: 24,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gap: 12, paddingInline: 16 },
  },
})

export const topBarStart = style({ alignItems: 'center', display: 'flex', gap: 8, minWidth: 0 })
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
      // A short window gives its height to the content, not the spacing around it.
      'screen and (max-height: 480px)': { gap: 16, paddingBlock: 16 },
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
    // The frame is the viewport's height, and a scroll table in it takes the rest.
    bounded: {
      false: {},
      true: { height: '100%', minHeight: 0 },
    },
    compact: {
      false: {},
      true: {
        gap: 16,
        padding: 24,
        '@media': {
          [`screen and (max-width: ${breakpoints.mobile})`]: { gap: 16, padding: '24px 16px' },
        },
      },
    },
  },
  defaultVariants: { bounded: false, compact: false },
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
