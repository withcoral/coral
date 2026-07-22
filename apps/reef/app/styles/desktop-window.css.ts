import { globalStyle, style, type GlobalStyleRule } from '@vanilla-extract/css'

const MACOS_DESKTOP = "html[data-coral-desktop-platform='darwin']"

// csstype omits Electron's Chromium-only app-region property.
function electronAppRegion(
  value: 'drag' | 'no-drag',
): GlobalStyleRule & { WebkitAppRegion: 'drag' | 'no-drag' } {
  return { WebkitAppRegion: value }
}

export const dragRegion = style({
  display: 'none',
})

// Electron places the native controls at x=14. The sidebar already contributes
// 12px of outer padding, so this inset starts its interactive header content at
// x=84 with a clear gap after the traffic lights.
globalStyle(`${MACOS_DESKTOP} [data-coral-sidebar]`, {
  flexBasis: '220px',
})

globalStyle(`${MACOS_DESKTOP} [data-coral-sidebar-header]`, {
  ...electronAppRegion('drag'),
  paddingInlineStart: '72px',
})

globalStyle(`${MACOS_DESKTOP} [data-coral-sidebar][data-sidebar-minimized='true']`, {
  flexBasis: '84px',
  minWidth: '84px',
})

globalStyle(
  `${MACOS_DESKTOP} [data-coral-sidebar][data-sidebar-minimized='true'] [data-coral-sidebar-header]`,
  {
    paddingBlockStart: '22px',
    paddingInlineStart: 0,
  },
)

// Keep this lower layer out of a local stacking context. Electron combines it
// with the explicit no-drag controls painted above it.
globalStyle(`${MACOS_DESKTOP} ${dragRegion}`, {
  ...electronAppRegion('drag'),
  WebkitUserSelect: 'none',
  display: 'block',
  height: '48px',
  insetBlockStart: 0,
  insetInline: 0,
  position: 'fixed',
  zIndex: -1,
})

globalStyle(`${MACOS_DESKTOP} [data-coral-window-error]`, {
  paddingBlockEnd: '24px',
  paddingBlockStart: '56px',
  paddingInline: '24px',
})

globalStyle(
  `${MACOS_DESKTOP} button, ${MACOS_DESKTOP} a, ${MACOS_DESKTOP} input, ${MACOS_DESKTOP} select, ${MACOS_DESKTOP} textarea, ${MACOS_DESKTOP} [role='button'], ${MACOS_DESKTOP} [role='tab'], ${MACOS_DESKTOP} [contenteditable='true']`,
  {
    ...electronAppRegion('no-drag'),
  },
)
