import { resetStyle } from '@/wax/css'

import '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

resetStyle('body', {
  backgroundColor: theme.surface.mainContent,
  color: theme.content.primary,
  fontFamily: theme.typography.body.fontFamily,
  fontSize: theme.typography.body.fontSize,
  lineHeight: theme.typography.body.lineHeight,
  margin: 0,
  minHeight: '100vh',
})

resetStyle('::selection', {
  backgroundColor: theme.content.selection,
  color: theme.content.primary,
})

resetStyle('::-webkit-scrollbar', {
  height: '8px',
  width: '8px',
})

resetStyle('::-webkit-scrollbar-track', {
  backgroundColor: theme.surface.main,
})

resetStyle('::-webkit-scrollbar-corner', {
  backgroundColor: theme.surface.main,
  borderRadius: '0 0 8px 0',
})

resetStyle('::-webkit-scrollbar-thumb', {
  backgroundColor: theme.surface.onMainContent,
  borderRadius: '8px',
})

resetStyle('::-webkit-scrollbar-thumb:hover', {
  backgroundColor: theme.surface.onMainContentHover,
})
