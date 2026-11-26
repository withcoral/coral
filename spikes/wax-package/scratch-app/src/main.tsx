import '@withcoral/wax/theme/reset.css'
import '@withcoral/wax/theme/global.css'
import { createRoot } from 'react-dom/client'

import { App } from './app'

createRoot(document.getElementById('root')!).render(<App />)
