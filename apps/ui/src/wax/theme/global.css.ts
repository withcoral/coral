import { globalStyle } from '@vanilla-extract/css'

import '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

export const WAX_UI_DATA_ATTRIBUTE = 'data-wax'
const isWaxUI = `body[${WAX_UI_DATA_ATTRIBUTE}="true"]`

globalStyle(`${isWaxUI} ::selection`, {
  backgroundColor: theme.content.selection,
  color: theme.content.primary,
})

globalStyle(`${isWaxUI} ::-webkit-scrollbar`, {
  height: '8px',
  width: '8px',
})

globalStyle(`${isWaxUI} ::-webkit-scrollbar-track`, {
  backgroundColor: theme.surface.main,
})

globalStyle(`${isWaxUI} ::-webkit-scrollbar-corner`, {
  backgroundColor: theme.surface.main,
  borderRadius: '0 0 8px 0',
})

globalStyle(`${isWaxUI} ::-webkit-scrollbar-thumb`, {
  backgroundColor: theme.surface.onMainContent,
  borderRadius: '8px',
})

globalStyle(`${isWaxUI} ::-webkit-scrollbar-thumb:hover`, {
  backgroundColor: theme.surface.onMainContentHover,
})
