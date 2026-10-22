import { globalStyle, style, styleVariants } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const root = style({
  ...theme.typography.bodySmall,
  minWidth: 0,
  overflowWrap: 'anywhere',
})

export const color = styleVariants({
  secondary: { color: theme.content.secondary },
  tertiary: { color: theme.content.tertiary },
})

globalStyle(`${root} > :first-child`, { marginBlockStart: 0 })
globalStyle(`${root} > :last-child`, { marginBlockEnd: 0 })
globalStyle(`${root} p`, { marginBlock: 4 })
globalStyle(`${root} ol, ${root} ul`, { marginBlock: 4, paddingInlineStart: 20 })
globalStyle(`${root} li`, { marginBlock: 2 })
globalStyle(`${root} ul`, { listStyleType: 'disc' })
globalStyle(`${root} ol`, { listStyleType: 'decimal' })
globalStyle(`${root} a`, {
  color: theme.content.link,
  textDecoration: 'underline',
  overflowWrap: 'anywhere',
})
globalStyle(`${root} a:hover`, { color: theme.content.linkHover })
globalStyle(`${root} a:focus-visible`, {
  outline: `2px solid ${theme.stroke.focused}`,
  outlineOffset: 2,
})
globalStyle(`${root} code`, {
  ...theme.typography.codeSmallInline,
  background: theme.content.code.inlineBackground,
  borderRadius: 4,
  color: theme.content.code.inlineColor,
  padding: '1px 4px',
})
globalStyle(`${root} strong`, { ...theme.typography.bodySmallStrong, color: theme.content.primary })
globalStyle(`${root} pre`, { overflowX: 'auto', marginBlock: 8 })
globalStyle(`${root} h1`, {
  ...theme.typography.headingLarge,
  color: theme.content.primary,
  marginBlock: 12,
})
globalStyle(`${root} h2`, {
  ...theme.typography.headingMedium,
  color: theme.content.primary,
  marginBlock: 12,
})
globalStyle(`${root} h3, ${root} h4, ${root} h5, ${root} h6`, {
  ...theme.typography.headingSmall,
  color: theme.content.primary,
  marginBlock: 8,
})
