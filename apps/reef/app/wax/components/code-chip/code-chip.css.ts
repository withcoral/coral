import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const chip = style({
  background: theme.content.code.inlineBackground,
  borderRadius: 6,
  color: theme.content.code.inlineColor,
  overflowWrap: 'anywhere',
  padding: '2px 6px',
  ...theme.typography.codeInline,
})
