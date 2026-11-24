import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const divider = style({
  flexShrink: 0,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  height: 13,
  cursor: 'row-resize',
  touchAction: 'none',
  borderTop: `1px solid ${theme.stroke.secondary}`,
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  background: theme.surface.onMainContentSubtle,
  selectors: {
    '&:hover, &:focus-visible': {
      background: theme.surface.onMainContentHover,
    },
    // Drawn inside: the divider spans its container, which clips a ring outside it.
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: -2 },
  },
})
export const grip = style({
  display: 'flex',
  color: theme.content.tertiary,
})
