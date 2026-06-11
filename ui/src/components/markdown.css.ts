import { globalStyle, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const root = style({
  color: theme.content.secondary,
  fontSize: 12,
  lineHeight: 1.5,
})

globalStyle(`${root} > :first-child`, { marginBlockStart: 0 })
globalStyle(`${root} > :last-child`, { marginBlockEnd: 0 })
globalStyle(`${root} p`, { marginBlock: 4 })
globalStyle(`${root} ol, ${root} ul`, { marginBlock: 4, paddingInlineStart: 20 })
globalStyle(`${root} li`, { marginBlock: 2 })
globalStyle(`${root} a`, { color: theme.content.info, textDecoration: 'underline' })
globalStyle(`${root} code`, {
  background: theme.surface.onMainContent,
  borderRadius: 4,
  color: theme.content.primary,
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
  fontSize: 11,
  padding: '1px 4px',
})
globalStyle(`${root} strong`, { color: theme.content.primary, fontWeight: 600 })
