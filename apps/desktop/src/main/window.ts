import { app, BrowserWindow, nativeTheme, session, shell } from 'electron'

import {
  consentHandOff,
  formWindowStep,
  navigationDecision,
  permissionGranted,
  windowOpenDecision,
} from './content-policy'
import { SURFACE } from './local-pages'

/** How long a hidden form window may stay hidden before Desktop closes it. */
const FORM_WINDOW_TIMEOUT_MS = 60_000

/** Reef's surface color, so a window never flashes another color as it opens. */
const surface = () => (nativeTheme.shouldUseDarkColors ? SURFACE.dark : SURFACE.light)

/** Desktop's own version and update flow, when it can update itself. */
export interface ReleaseUpdates {
  runningVersion: string
  /** Answers a click on Reef's notice of a newer release. */
  offer: (version: string) => void
}

/**
 * Applies the content policy to every web contents Electron creates, including
 * any this app does not create itself. `reefOrigin` answers the current Reef
 * origin, or null before Reef is ready. `log` records what the user cannot see.
 */
export function installContentGuards(
  reefOrigin: () => string | null,
  log: (message: string) => void,
  updates: ReleaseUpdates | undefined,
): void {
  const guard = (event: Electron.Event, url: string) => {
    const decision = navigationDecision(url, reefOrigin())
    if (decision === 'allow') return
    event.preventDefault()
    if (decision === 'open-externally') void shell.openExternal(url)
  }

  // Hidden windows that submit a Reef form meant for a new tab. Each one waits
  // for Reef's answer, and `formWindowStep` decides what happens next.
  const formWindows = new Set<number>()
  const openConsent = (url: string) =>
    shell
      .openExternal(url)
      .catch((error: unknown) => log(`consent: the system browser did not open: ${String(error)}`))

  app.on('web-contents-created', (_event, contents) => {
    contents.setWindowOpenHandler(({ url, postBody }) => {
      const decision = windowOpenDecision(
        url,
        postBody !== undefined,
        reefOrigin(),
        updates?.runningVersion ?? null,
      )
      if (decision === 'submit-hidden')
        return {
          action: 'allow',
          overrideBrowserWindowOptions: { show: false, title: 'Coral', backgroundColor: surface() },
        }
      if (decision === 'open-externally') void shell.openExternal(url)
      else if (typeof decision === 'object') updates?.offer(decision.offerUpdate)
      return { action: 'deny' }
    })
    // Electron emits this as it creates the window, before Reef can answer.
    contents.on('did-create-window', (window) => {
      const id = window.webContents.id
      formWindows.add(id)
      // A hidden window keeps Linux and Windows from quitting when the person
      // closes Coral's window, so one that never finishes must not stay.
      const timeout = setTimeout(() => {
        if (!formWindows.has(id)) return
        log('sign-in: the form window did not finish in time')
        window.destroy()
      }, FORM_WINDOW_TIMEOUT_MS)
      window.once('closed', () => {
        clearTimeout(timeout)
        formWindows.delete(id)
      })
      window.on('page-title-updated', (event) => event.preventDefault())
      // This includes a redirect that the guard stops, which would otherwise
      // show an empty window.
      window.webContents.on('did-fail-load', (_event, code, description, _url, isMainFrame) => {
        if (!isMainFrame || !formWindows.has(id)) return
        log(`sign-in: the form window did not load: ${description} (${code})`)
        window.destroy()
      })
    })
    contents.on('will-navigate', guard)
    contents.on('will-redirect', guard)
    // will-navigate covers only the main frame. A frame stays on Reef's origin
    // and never opens anything externally.
    contents.on('will-frame-navigate', (event) => {
      if (!event.isMainFrame && navigationDecision(event.url, reefOrigin()) !== 'allow')
        event.preventDefault()
    })
    contents.on('will-attach-webview', (event) => event.preventDefault())
  })

  session.defaultSession.webRequest.onHeadersReceived((details, callback) => {
    const origin = reefOrigin()
    const handOff =
      origin === null
        ? { action: 'pass' as const }
        : consentHandOff(
            {
              method: details.method,
              resourceType: details.resourceType,
              url: details.url,
              statusCode: details.statusCode,
              headers: details.responseHeaders ?? {},
            },
            origin,
          )
    const formWindow =
      details.resourceType === 'mainFrame' &&
      details.webContents !== undefined &&
      formWindows.has(details.webContents.id)
        ? BrowserWindow.fromWebContents(details.webContents)
        : null
    if (formWindow) {
      switch (formWindowStep(handOff.action, details.statusCode)) {
        case 'wait':
          return callback({})
        case 'show': {
          // The window stays hidden, and under the timeout, until the page has
          // loaded. A page that fails or stalls closes it instead.
          const id = formWindow.webContents.id
          formWindow.webContents.once('did-finish-load', () => {
            if (formWindows.delete(id)) formWindow.show()
          })
          return callback({})
        }
        case 'close':
          formWindows.delete(formWindow.webContents.id)
          // The page that opened the form already shows the sign-in progress.
          if (handOff.action === 'hand-off') void openConsent(handOff.consent)
          else log('consent: Desktop cannot hand off the sign-in that Reef marked')
          callback({ cancel: true })
          return formWindow.destroy()
      }
    }
    switch (handOff.action) {
      case 'pass':
        return callback({})
      case 'refuse':
        return callback({ cancel: true })
      case 'hand-off':
        // The window still shows sign-in progress, where the user can try
        // again.
        void openConsent(handOff.consent)
        return callback({ statusLine: handOff.statusLine, responseHeaders: handOff.headers })
    }
  })

  session.defaultSession.setPermissionRequestHandler((_contents, permission, callback, details) =>
    callback(permissionGranted(permission, details.requestingUrl, reefOrigin())),
  )
  session.defaultSession.setPermissionCheckHandler((_contents, permission, requestingOrigin) =>
    permissionGranted(permission, requestingOrigin, reefOrigin()),
  )
}

export function createMainWindow(icon: string): BrowserWindow {
  const window = new BrowserWindow({
    width: 1280,
    height: 860,
    minWidth: 720,
    minHeight: 520,
    title: 'Coral',
    icon,
    // Shown once its first page has drawn, on Reef's own surface color.
    show: false,
    backgroundColor: surface(),
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  })
  // Reef pages have no titles of their own, and Electron would otherwise keep
  // the title of whichever page last set one.
  window.on('page-title-updated', (event) => event.preventDefault())
  return window
}
