import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { RouterProvider } from 'react-router'

import { createAppRouter } from './routes/router'
import './index.css'

const root = document.querySelector<HTMLDivElement>('#app')

if (!root) {
  throw new Error('Missing #app root')
}

document.body.setAttribute('data-wax', 'true')

function migrateLegacyHashRoute() {
  const { hash } = window.location
  if (!hash.startsWith('#/')) return

  const legacyPath = hash.replace(/^#/, '')
  if (legacyPath === '/sources' || legacyPath === '/traces') {
    window.history.replaceState(null, '', legacyPath)
  }
}

migrateLegacyHashRoute()

const router = createAppRouter()

createRoot(root).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
)
