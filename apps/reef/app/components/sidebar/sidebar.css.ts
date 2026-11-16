import { keyframes, style } from '@vanilla-extract/css'
import { animation, theme, zIndex } from '@/wax/theme/theme.css'
import { FRAME_INSET_PX } from '@/components/content-container/constants'
import { TOP_BAR_HEIGHT_PX } from '@/wax/components/page-layout/constants'

const SIDEBAR_PADDING_PX = 12
// As tall as the collapse toggle beside the brand.
const IDENTITY_HEIGHT_PX = 32
// The masthead sits so the brand centres on the top bar of the page's frame
// beside it, which starts a little below the window's edge.
const MASTHEAD_OFFSET_PX =
  FRAME_INSET_PX + (TOP_BAR_HEIGHT_PX - IDENTITY_HEIGHT_PX) / 2 - SIDEBAR_PADDING_PX
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
export const sidebarMinimized = style({ flexBasis: 68, alignItems: 'center' })
// After a toggle, the width moves like a drawer and the contents cross-blur, which
// hides their swap between layouts. Only a toggle animates, never a page load.
const { panel } = animation
const expandBlur = keyframes({ from: { filter: panel.blur } })
const collapseBlur = keyframes({ from: { filter: panel.blur } })
export const sidebarResized = style({
  animation: `${expandBlur} ${panel.open} ${panel.ease}`,
  overflowX: 'clip',
  transition: `flex-basis ${panel.open} ${panel.ease}`,
  selectors: {
    [`&${sidebarMinimized}`]: {
      animation: `${collapseBlur} ${panel.close} ${panel.ease}`,
      transition: `flex-basis ${panel.close} ${panel.ease}`,
    },
  },
  '@media': {
    '(prefers-reduced-motion: reduce)': {
      animation: 'none',
      transition: 'none',
      selectors: { [`&${sidebarMinimized}`]: { animation: 'none', transition: 'none' } },
    },
  },
})
// In the drawer the sidebar fills the popup, and looks as it does on a desktop.
export const drawerSidebar = style({ flex: 1, height: 'auto', minHeight: 0 })
// A masthead: the brand with the collapse toggle at its end, and room beneath
// it before the workspace switcher. Collapsed, the toggle sits under the mark, so it
// stays at the top in both states.
export const header = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  marginBlockStart: MASTHEAD_OFFSET_PX,
  minWidth: 0,
})
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
// The switcher and the current Workspace's pages take the spare height. Only
// the pages scroll, so the switcher and the footer stay put.
export const workspace = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 8,
  minHeight: 0,
})
// A fine ring sets the switcher apart from the pages beneath it: it opens a
// menu, and they open pages.
export const switcher = style({
  boxShadow: `inset 0 0 0 1px ${theme.stroke.primary}`,
  // As tall with an icon as with the 20px avatar.
  minHeight: 32,
})
// The scroll area runs into the sidebar's 12px end padding (see its width in
// sidebar.tsx), so its scrollbar sits by the sidebar's edge. The list restores
// the padding for its rows.
export const destinationsScroll = style({ flex: '1 1 auto', minHeight: 0 })
export const destinations = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  paddingInlineEnd: 12,
})
// The settings nav: the way back, then the organization's sections under their
// label. It scrolls as the workspace's pages do.
export const settingsNav = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  paddingInlineEnd: 12,
})
export const settingsGroup = style({ display: 'flex', flexDirection: 'column', gap: 2 })
export const settingsGroupLabel = style({
  color: theme.content.tertiary,
  paddingBlock: 6,
  paddingInline: 8,
})
// The switcher's menu holds workspace names. It is wider than a menu of short commands.
export const menu = style({ width: 240 })
// A Workspace's row in the switcher's menu: the Workspace, and the action that
// copies its MCP URL.
export const menuRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: 2,
  position: 'relative',
})
// The Workspace takes the row's spare width, so a long name truncates before the action.
export const menuRowItem = style({ flex: '1 1 0', minWidth: 0, width: 'auto' })
const pointer = '(hover: hover)'
// With a pointer, the action takes the avatar's place while you point at the
// row or reach the action with the keyboard, so the row keeps its full width.
// A touch device cannot point, so there the action stays at the row's end.
export const menuRowAction = style({
  '@media': {
    [pointer]: {
      insetBlockStart: 0,
      // Centres the 32px action on the avatar, which starts 7px into the row.
      insetInlineStart: 1,
      position: 'absolute',
      selectors: {
        [`${menuRow}:not(:hover) &:not([data-highlighted])`]: { opacity: 0 },
      },
    },
  },
})
// A 20px avatar takes an 18px icon's room, so the names line up under one another.
export const menuAvatar = style({
  display: 'flex',
  flexShrink: 0,
  marginInline: -1,
  '@media': {
    [pointer]: {
      selectors: {
        [`${menuRow}:hover &, ${menuRow}:has(${menuRowAction}[data-highlighted]) &`]: {
          opacity: 0,
        },
      },
    },
  },
})
// Keeps a count clear of the menu item's edge, as its check mark is.
export const menuCount = style({ display: 'flex', flexShrink: 0, marginInlineEnd: 4 })
// Collapsed, the switcher is the current Workspace's avatar, with its pages
// beneath it.
export const rail = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 8,
  minHeight: 0,
  overflowY: 'auto',
  scrollbarWidth: 'none',
})
// A 34px square holds the 28px avatar, larger than the icons beneath it.
export const railTile = style({ borderRadius: 10, height: 34, paddingBlock: 0 })
// The release notice and the help and account menus are rows like the pages above them.
export const footer = style({ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 })
// Collapsed, the brand is the same centered square as the buttons, so they
// share one axis.
export const minimizedControl = style({
  justifyContent: 'center',
  minWidth: 0,
  paddingInline: 0,
  flex: 'none',
  width: 34,
})
export const minimizedColumn = style({ alignItems: 'center', flexDirection: 'column' })
export const railDestinations = style({
  alignItems: 'center',
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
})
