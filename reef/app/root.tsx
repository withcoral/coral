import { isRouteErrorResponse, Links, Meta, Outlet, Scripts, ScrollRestoration } from 'react-router'

import type { Route } from './+types/root'
import { readSidebarCollapsedCookie } from './components/sidebar/sidebar-state'
import { readSidebarCollapsedCookieValue } from './components/sidebar/sidebar-state'
import { ensureCoralRuntime, installCoralRuntimeFetchBridge } from './lib/coral-runtime'
import './styles/globals.css'
import './wax/theme/global.css'
import { darkTheme } from './wax/theme/theme-dark.css'
import { lightTheme } from './wax/theme/theme-light.css'
import { THEME_STORAGE_KEY, useTheme } from './wax/theme/theme-provider'

installCoralRuntimeFetchBridge()

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
      <ThemedBody>{children}</ThemedBody>
    </html>
  )
}

function ThemedBody({ children }: { children: React.ReactNode }) {
  const { theme, themeClass } = useTheme()

  return (
    <body className={themeClass} style={{ colorScheme: theme }} suppressHydrationWarning>
      <ThemeBootstrapScript />
      {children}
      <ScrollRestoration />
      <Scripts />
    </body>
  )
}

function ThemeBootstrapScript() {
  const source = `
(function () {
  var darkClass = ${JSON.stringify(darkTheme)};
  var lightClass = ${JSON.stringify(lightTheme)};
  var storageKey = ${JSON.stringify(THEME_STORAGE_KEY)};
  function readPreference() {
    try {
      var stored = window.localStorage.getItem(storageKey);
      var parsed = stored ? JSON.parse(stored) : 'system';
      return parsed === 'dark' || parsed === 'light' || parsed === 'system' ? parsed : 'system';
    } catch (_) {
      return 'system';
    }
  }
  var preference = readPreference();
  var theme = preference === 'system'
    ? (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark')
    : preference;
  document.body.classList.remove(darkClass, lightClass);
  document.body.classList.add(theme === 'light' ? lightClass : darkClass);
  document.body.style.colorScheme = theme;
})();
`
  return <script dangerouslySetInnerHTML={{ __html: source }} />
}

export async function loader({ request }: Route.LoaderArgs) {
  return {
    sidebarIsMinimized: readSidebarCollapsedCookie(request),
  }
}

export async function clientLoader(_args: Route.ClientLoaderArgs) {
  await ensureCoralRuntime()
  return {
    sidebarIsMinimized:
      typeof document === 'undefined' ? false : readSidebarCollapsedCookieValue(document.cookie),
  }
}

clientLoader.hydrate = true as const

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
