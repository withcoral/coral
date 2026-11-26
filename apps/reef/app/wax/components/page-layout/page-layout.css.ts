import { recipe, style } from '../../css'

import { theme } from '../../theme/theme.css'

import { breakpoints } from '../../styles/breakpoints'

import { TOP_BAR_HEIGHT_PX } from './constants'

// A breadcrumb link's height: the one row the bar holds.
const LINK_HEIGHT_PX = 32

const mobile = `screen and (max-width: ${breakpoints.mobile})`
// A page with a panel puts the panel under the content in a window this narrow.
const stacked = `screen and (max-width: ${breakpoints.md})`
// Beside a panel, the content and the panel's content keep this far from the split.
const PANEL_GUTTER_PX = 48

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

// Breadcrumbs give way to the actions. Other start content, such as a brand,
// keeps its size, and the actions give way instead.
export const topBarStart = recipe({
  base: { alignItems: 'center', display: 'flex', gap: 8, minWidth: 0 },
  variants: { custom: { true: { flexShrink: 0 }, false: {} } },
})
export const topBarActions = recipe({
  base: { alignItems: 'center', display: 'flex', gap: 4 },
  variants: { custom: { true: { minWidth: 0 }, false: {} } },
})
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
    // Beside a panel, the content ends at the split, as wide as a form. The
    // page's other gutters stay.
    beside: {
      false: {},
      true: {
        justifySelf: 'end',
        marginInline: 0,
        maxWidth: 480,
        paddingInline: PANEL_GUTTER_PX,
        '@media': {
          [stacked]: { justifySelf: 'stretch', maxWidth: 'none', paddingInline: 24 },
          [mobile]: { paddingInline: 16 },
        },
      },
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
  defaultVariants: { beside: false, bounded: false, compact: false },
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

// The content and the panel, each half the width. The scroll area's content is
// a flex row, so this takes all of it.
export const withPanel = style({
  display: 'grid',
  flex: '1 1 auto',
  gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)',
  '@media': {
    [stacked]: { gridTemplateColumns: 'minmax(0, 1fr)', gridTemplateRows: 'auto 1fr' },
  },
})

// In the light theme, so its own text and controls are light too. The page's
// gutters, as the frame's are.
export const panel = style({
  backgroundColor: theme.surface.card,
  borderInlineStart: `1px solid ${theme.stroke.mainContent}`,
  color: theme.content.primary,
  padding: `32px ${PANEL_GUTTER_PX}px`,
  '@media': {
    [stacked]: {
      borderBlockStart: `1px solid ${theme.stroke.mainContent}`,
      borderInlineStart: 'none',
      paddingInline: 24,
    },
    [mobile]: { paddingBlock: 24, paddingInline: 16 },
    'screen and (max-height: 480px)': { paddingBlock: 16 },
  },
})

export const panelContent = style({
  maxWidth: 560,
  '@media': { [stacked]: { maxWidth: 'none' } },
})

export const splitBody = style({ flex: '1 1 auto', minHeight: 0, minWidth: 0 })
