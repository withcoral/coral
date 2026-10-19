import { style } from '@/wax/css'
import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'
const mobileQuery = `screen and (max-width: ${breakpoints.mobile})`
export const desktop = style({
  display: 'contents',
  '@media': { [mobileQuery]: { display: 'none' } },
})
export const mobile = style({
  display: 'none',
  '@media': { [mobileQuery]: { display: 'flex', flexShrink: 0 } },
})
// The drawer is the sidebar itself: it brings its own background and padding,
// so the popup gives up its chrome. The attribute outranks the dialog's own rules.
export const drawer = style({
  width: 220,
  selectors: {
    '&[role="dialog"]': {
      backgroundColor: theme.surface.main,
      border: 0,
      gap: 0,
      padding: 0,
    },
  },
})
