import { isRouteErrorResponse, Links, Meta, Outlet, Scripts, ScrollRestoration } from 'react-router'

import type { Route } from './+types/root'
import './styles/globals.css'
import './wax/theme/global.css'
import { darkTheme } from './wax/theme/theme-dark.css'

export const links = () => [
  {
    href: '/coral-light.svg',
    media: '(prefers-color-scheme: light)',
    rel: 'icon',
    type: 'image/svg+xml',
  },
  {
    href: '/coral-dark.svg',
    media: '(prefers-color-scheme: dark)',
    rel: 'icon',
    type: 'image/svg+xml',
  },
  {
    href: '/favicon.ico',
    rel: 'alternate icon',
    sizes: '48x48 32x32 16x16',
  },
]

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
      </head>
      <body className={darkTheme}>
        {children}
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  )
}

export default function App() {
  return <Outlet />
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  let message = 'Oops!'
  let details = 'An unexpected error occurred.'
  let stack: string | undefined

  if (isRouteErrorResponse(error)) {
    message = error.status === 404 ? '404' : 'Error'
    details =
      error.status === 404 ? 'The requested page could not be found.' : error.statusText || details
  } else if (import.meta.env.DEV && error && error instanceof Error) {
    details = error.message
    stack = error.stack
  }

  return (
    <main>
      <h1>{message}</h1>
      <p>{details}</p>
      {stack && (
        <pre>
          <code>{stack}</code>
        </pre>
      )}
    </main>
  )
}
