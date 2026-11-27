import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

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
export const railDestinations = style({
  alignItems: 'center',
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
})
