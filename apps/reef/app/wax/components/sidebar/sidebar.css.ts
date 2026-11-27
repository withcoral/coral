import { keyframes, style } from '@/wax/css'
import { TOP_BAR_CENTRE_PX } from '@/wax/components/app-frame'
import { animation, theme, zIndex } from '@/wax/theme/theme.css'

const SIDEBAR_PADDING_PX = 12
// As tall as the collapse toggle beside the brand.
const IDENTITY_HEIGHT_PX = 32
// The masthead sits so the brand centres on the top bar of the page's frame
// beside it, which starts a little below the window's edge, under the frame's line.
const MASTHEAD_OFFSET_PX = TOP_BAR_CENTRE_PX - IDENTITY_HEIGHT_PX / 2 - SIDEBAR_PADDING_PX

export const sidebar = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flexDirection: 'column',
  flex: '0 0 220px',
  height: '100dvh',
  minWidth: 0,
  padding: SIDEBAR_PADDING_PX,
  gap: 16,
  zIndex: zIndex.navigation,
})
export const minimized = style({ flexBasis: 68, alignItems: 'center' })
// After a toggle, the width moves like a drawer and the contents cross-blur, which
// hides their swap between layouts. Only a toggle animates, never a page load.
const { panel } = animation
const expandBlur = keyframes({ from: { filter: panel.blur } })
const collapseBlur = keyframes({ from: { filter: panel.blur } })
export const resized = style({
  animation: `${expandBlur} ${panel.open} ${panel.ease}`,
  overflowX: 'clip',
  transition: `flex-basis ${panel.open} ${panel.ease}`,
  selectors: {
    [`&${minimized}`]: {
      animation: `${collapseBlur} ${panel.close} ${panel.ease}`,
      transition: `flex-basis ${panel.close} ${panel.ease}`,
    },
  },
  '@media': {
    '(prefers-reduced-motion: reduce)': {
      animation: 'none',
      transition: 'none',
      selectors: { [`&${minimized}`]: { animation: 'none', transition: 'none' } },
    },
  },
})
// In the drawer the sidebar fills the popup, and looks as it does on a desktop.
export const drawer = style({ flex: 1, height: 'auto', minHeight: 0 })
// A masthead: the brand with the collapse toggle at its end. Collapsed, the
// toggle sits under the mark, so it stays at the top in both states.
export const masthead = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  marginBlockStart: MASTHEAD_OFFSET_PX,
  minWidth: 0,
})
export const mastheadMinimized = style({ alignItems: 'center', flexDirection: 'column' })
// The 22px mark centres on the 18px icons of the buttons below, so every icon
// shares one column.
export const identity = style({
  display: 'flex',
  alignItems: 'center',
  flex: 1,
  minHeight: IDENTITY_HEIGHT_PX,
  paddingInline: 6,
  color: theme.content.primary,
  textDecoration: 'none',
})
// Collapsed, the brand is the same centered square as the buttons, so they
// share one axis.
export const identityMinimized = style({
  justifyContent: 'center',
  minWidth: 0,
  paddingInline: 0,
  flex: 'none',
  width: 34,
})
