import { useEffect } from 'react'
import { isRouteErrorResponse, Links, Meta, Outlet, Scripts, ScrollRestoration } from 'react-router'

import type { Route } from './+types/root'
import {
  readSidebarCollapsedCookie,
  readSidebarCollapsedCookieValue,
} from './components/sidebar/sidebar-state'
import { NavigationProgressBar } from './components/navigation-progress-bar'
import { ErrorPage } from './components/error-page/error-page'
import { runtimeConfigMiddleware } from './lib/runtime-config.server'
import resetStylesheet from './wax/theme/reset.css?url'
import './wax/theme/global.css'
import { darkTheme } from './wax/theme/theme-dark.css'
import { lightTheme } from './wax/theme/theme-light.css'
import { PageTitleRoot } from './wax/components/page-title'
import { THEME_STORAGE_KEY, useTheme } from './wax/theme/theme-provider'

export const links = () => [
  // Discover body and heading fonts before the first text paint, not during layout.
  ...[400, 500].map((weight) => ({
    rel: 'preload',
    href: `/fonts/encode-sans-semi-expanded/EncodeSansSemiExpanded-${weight}-latin.woff2`,
    as: 'font',
    type: 'font/woff2',
    crossOrigin: 'anonymous' as const,
  })),
  // Keep @font-face rules loaded when development hydration removes critical CSS.
  { rel: 'stylesheet', href: resetStylesheet },
  { href: '/favicon.svg', rel: 'icon', type: 'image/svg+xml' },
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
  // A server-rendered page shows before React attaches its handlers. React runs
  // this effect after every route's effects, so the marker means input works.
  // Reel's waitForHydration reads it.
  useEffect(() => {
    document.documentElement.dataset.hydrated = ''
  }, [])

  return (
    <body className={themeClass} style={{ colorScheme: theme }} suppressHydrationWarning>
      <ThemeBootstrapScript />
      <PageTitleRoot value="Coral">{children}</PageTitleRoot>
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

// Every route, public or protected, reads the configuration resolved at startup.
export const middleware: Route.MiddlewareFunction[] = [runtimeConfigMiddleware]

export async function loader({ request }: Route.LoaderArgs) {
  return {
    sidebarIsMinimized: readSidebarCollapsedCookie(request),
  }
}

// The server already reads this cookie. Leave hydration off to preserve native POST action data.
export async function clientLoader(_args: Route.ClientLoaderArgs) {
  return {
    sidebarIsMinimized: readSidebarCollapsedCookieValue(document.cookie),
  }
}

export default function App() {
  return (
    <>
      <NavigationProgressBar />
      <Outlet />
    </>
  )
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  let message = 'Coral cannot open this page'
  let details = 'Coral encountered an unexpected error. Reload the page.'
  let stack: string | undefined

  if (isRouteErrorResponse(error)) {
    message = error.status === 404 ? '404' : 'Error'
    details =
      error.status === 404
        ? 'This page does not exist. Check the URL.'
        : error.statusText || details
  } else if (import.meta.env.DEV && error && error instanceof Error) {
    details = error.message
    stack = error.stack
  }

  return <ErrorPage title={message} details={details} stack={stack} />
}
