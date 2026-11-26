import '@withcoral/wax/theme/global.css'
import { renderToString } from 'react-dom/server'
// A font the app preloads, as Reef's root does from `public/fonts` today.
import font from '@withcoral/wax/fonts/encode-sans-semi-expanded/EncodeSansSemiExpanded-400-latin.woff2?url'

import { App } from './app'

export function render() {
  return { font, html: renderToString(<App />) }
}
