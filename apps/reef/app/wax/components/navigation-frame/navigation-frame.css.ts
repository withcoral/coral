import { style } from '@/wax/css'
import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'
const mobileQuery = `screen and (max-width: ${breakpoints.mobile})`
export const desktop = style({
  display: 'contents',
  '@media': { [mobileQuery]: { display: 'none' } },
})
export const mobile = style({ display: 'none', '@media': { [mobileQuery]: { display: 'block' } } })
export const header = style({
  display: 'flex',
  alignItems: 'center',
  gap: 12,
  padding: '8px 12px',
  background: theme.surface.main,
  borderBottom: `1px solid ${theme.stroke.secondary}`,
})
export const drawer = style({ width: 280 })
