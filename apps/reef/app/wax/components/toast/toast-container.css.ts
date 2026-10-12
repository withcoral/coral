import { globalStyle, style } from '@/wax/css'
import { zIndex } from '@/wax/theme/theme.css'

// react-toastify's own z-index sits below dialogs, which hid a toast raised from one.
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
