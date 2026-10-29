import { keyframes, style, styleVariants } from '@vanilla-extract/css'
import { animation, theme, zIndex } from '@/wax/theme/theme.css'
import { TOP_BAR_HEIGHT_PX } from '@/wax/components/page-layout/constants'

const SIDEBAR_PADDING_PX = 12
// As tall as the collapse toggle beside the brand.
const IDENTITY_HEIGHT_PX = 32
// The masthead rises into the sidebar's padding, so the brand centres on the
// page's top bar beside it.
const MASTHEAD_OFFSET_PX = (TOP_BAR_HEIGHT_PX - IDENTITY_HEIGHT_PX) / 2 - SIDEBAR_PADDING_PX
// Puts a 22px icon button's centre under the 30px collapse toggle's, so the
// trailing controls share one column.
const TRAILING_INSET_PX = 3
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
// it before the scopes begin. Collapsed, the toggle sits under the mark, so it
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
export const spacer = style({ flex: 1, minHeight: 0 })
// The Workspace tree takes the spare height. Its label stays put and only the
// list scrolls, so the footer stays put too.
export const tree = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 4,
  minHeight: 0,
})
// The scroll area runs into the sidebar's 12px end padding (see its width in
// sidebar.tsx), so its scrollbar sits by the sidebar's edge. The list restores
// the padding for its rows.
export const treeScroll = style({ flex: '1 1 auto', minHeight: 0 })
export const treeList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  paddingInlineEnd: 12,
})
export const groupLabel = style({
  ...theme.typography.bodySmall,
  alignItems: 'center',
  color: theme.content.tertiary,
  display: 'flex',
  justifyContent: 'space-between',
  minHeight: 28,
  // The create button centres on the collapse toggle above it.
  paddingInlineStart: 8,
  paddingInlineEnd: TRAILING_INSET_PX,
})
export const workspace = style({ display: 'flex', flexDirection: 'column', gap: 2 })
// The header and its actions share one grid cell, so the actions sit inside
// the header's trailing edge. Pointer devices reveal them on hover or focus;
// touch devices always show them.
// The column may shrink below the name's width, so the name truncates.
export const workspaceHeader = style({ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr)' })
export const headerButton = style({
  selectors: {
    [`${workspaceHeader} &`]: { gridArea: '1 / 1' },
    // Hovering an action still hovers the header it sits in.
    [`${workspaceHeader}:hover &`]: { background: theme.sidebar.button.hover },
  },
})
// Each 22px action plus the gap keeps a long name clear of the actions.
export const headerActionsSpace = styleVariants({
  0: {},
  1: { selectors: { [`${workspaceHeader} &`]: { paddingInlineEnd: 34 } } },
  2: { selectors: { [`${workspaceHeader} &`]: { paddingInlineEnd: 58 } } },
})
export const workspaceActions = style({
  alignItems: 'center',
  alignSelf: 'center',
  display: 'flex',
  gap: 2,
  gridArea: '1 / 1',
  justifySelf: 'end',
  marginInlineEnd: TRAILING_INSET_PX,
  '@media': {
    '(hover: hover)': {
      selectors: {
        [`${workspaceHeader}:not(:hover, :focus-within) &:not(:has([aria-current]))`]: {
          opacity: 0,
        },
      },
    },
  },
})
// Archived Workspaces sit below the active ones, set apart by a rule.
export const archived = style({
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  marginTop: 8,
  paddingTop: 8,
})
// Child icons line up under the Workspace name: the header's 18px chevron plus
// SidebarButton's 10px gap.
export const workspaceChildren = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  paddingInlineStart: 28,
})
// Collapsed, each Workspace is an avatar under a divider. The current one
// carries its destinations beneath it.
export const rail = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 4,
  minHeight: 0,
  overflowY: 'auto',
  scrollbarWidth: 'none',
})
export const railWorkspace = style({
  alignItems: 'center',
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  paddingTop: 6,
  width: 44,
  // A rule divides one Workspace from the next; the masthead's space already
  // sets the first apart, as it does in the expanded sidebar.
  selectors: { '&:first-child': { borderTop: 0, paddingTop: 0 } },
})
// Each Workspace is its avatar. A fine ring marks the current one, quieter
// than the selected background its current destination carries.
export const railTile = style({
  // Positions a pending-action dot in the corner.
  position: 'relative',
  alignItems: 'center',
  borderRadius: 10,
  display: 'flex',
  flex: 'none',
  height: 34,
  justifyContent: 'center',
  transition: animation.colorTransition,
  width: 34,
  selectors: {
    '&:hover': { background: theme.sidebar.button.hover },
    '&:focus-visible': { outline: `1px solid ${theme.button.primary.focus}` },
  },
})
export const railTileCurrent = style({
  boxShadow: `inset 0 0 0 1px ${theme.stroke.control}`,
})
// Help and the account menu are rows like the destinations above them.
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
