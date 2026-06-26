import { globalStyle } from '@vanilla-extract/css'

import '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

globalStyle('body', {
  backgroundColor: theme.surface.mainContent,
  color: theme.content.primary,
  fontFamily: theme.typography.body.fontFamily,
  fontSize: theme.typography.body.fontSize,
  lineHeight: theme.typography.body.lineHeight,
  margin: 0,
  minHeight: '100vh',
})

globalStyle('::selection', {
  backgroundColor: theme.content.selection,
  color: theme.content.primary,
})

globalStyle('::-webkit-scrollbar', {
  height: '8px',
  width: '8px',
})

globalStyle('::-webkit-scrollbar-track', {
  backgroundColor: theme.surface.main,
})

globalStyle('::-webkit-scrollbar-corner', {
  backgroundColor: theme.surface.main,
  borderRadius: '0 0 8px 0',
})

globalStyle('::-webkit-scrollbar-thumb', {
  backgroundColor: theme.surface.onMainContent,
  borderRadius: '8px',
})

globalStyle('::-webkit-scrollbar-thumb:hover', {
  backgroundColor: theme.surface.onMainContentHover,
})
