import { globalStyle, style } from '@vanilla-extract/css'

import { zIndex } from '@/wax/theme/theme.css'

export const toastContainer = style({
  zIndex: zIndex.notification,
})

// Override react-toastify default styles to use our custom toast styling
globalStyle(`${toastContainer} .Toastify__toast`, {
  background: 'transparent',
  boxShadow: 'none',
  minHeight: 'unset',
  padding: 0,
})
globalStyle(`${toastContainer} .Toastify__toast-body`, {
  margin: 0,
  padding: 0,
})
