import { style } from '@vanilla-extract/css'

import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

export const code = style({
  color: theme.content.primary,
  fontFamily: fontFamily.dmMono,
  fontWeight: 700,
})
