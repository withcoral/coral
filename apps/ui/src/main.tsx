import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App'
import './index.css'

const root = document.querySelector<HTMLDivElement>('#app')

if (!root) {
  throw new Error('Missing #app root')
}

document.body.setAttribute('data-wax', 'true')

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
