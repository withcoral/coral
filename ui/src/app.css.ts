import { globalStyle } from '@vanilla-extract/css'

import '@/wax/theme/global.css'
import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

globalStyle('body', {
  backgroundColor: theme.surface.main,
  color: theme.content.primary,
  fontFamily: fontFamily.inter,
  margin: 0,
  padding: 0,
})

globalStyle('*, *::before, *::after', {
  boxSizing: 'border-box',
})
