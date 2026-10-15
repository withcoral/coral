import { globalStyle, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// How far the bar floats above the list's foot.
const BAR_INSET_PX = 16
// Room at the list's end while the bar shows: one line of controls, its
// inset, and a gap above it, so the last rows scroll clear of it.
const ROOM_PX = 72

export const bar = style({
  alignItems: 'center',
  background: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: 10,
  bottom: BAR_INSET_PX,
  boxShadow: theme.elevation.e3,
  display: 'flex',
  gap: 8,
  // Centred over the list; never wider than it, so it wraps on a narrow screen.
  insetInline: 0,
  marginInline: 'auto',
  maxWidth: `calc(100% - ${BAR_INSET_PX * 2}px)`,
  paddingBlock: 6,
  paddingInline: '14px 6px',
  position: 'absolute',
  width: 'fit-content',
  zIndex: 3,
})

// The list's scroll port comes first; the bar, while it shows, floats over it.
export const frame = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  minHeight: 0,
  minWidth: 0,
  position: 'relative',
})
globalStyle(`${frame}:has(> ${bar}) > :first-child`, { paddingBlockEnd: ROOM_PX })
