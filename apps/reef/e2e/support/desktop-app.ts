// Launches the packaged Desktop app for one recorded session. Desktop finds its
// data folder as it does for a person, so the app has no test hook.
import { spawn } from 'node:child_process'
import { copyFile, cp } from 'node:fs/promises'
import { join } from 'node:path'

import { _electron, chromium, type ElectronApplication, type Page } from 'playwright'
import { expect, inject, type TestContext } from 'vitest'

import { withBrowserSession, type Session } from './browser-session.ts'
import { DESKTOP_DATA } from './config.ts'
import { freshDesktopProfile, type DesktopProfile } from './desktop-profile.ts'
import { descendantProcesses } from './os-processes.ts'
import { bounded } from './stack.ts'

export interface DesktopSession extends Session {
  app: ElectronApplication
  /** The loopback Reef origin the window shows. */
  reef: string
}

export type { DesktopProfile }

/**
 * Creates a fresh profile for every launch in one test. When the test
 * finishes, Desktop's logs join the run's evidence.
 */
export async function desktopProfile(test: TestContext): Promise<DesktopProfile> {
  const { profile, remove } = await freshDesktopProfile(inject('reel').desktop!)
  test.onTestFinished(async () => {
    const { runDir, target } = inject('reel')
    await cp(
      join(profile.appData, DESKTOP_DATA, 'logs'),
      join(runDir, target.name, test.task.id, 'logs'),
      { recursive: true },
    ).catch((error) => console.warn(`Desktop logs: ${String(error)}`))
    await remove?.()
  })
  return profile
}

/** How the app's main process ended. A hung Quit ends in the harness's SIGKILL. */
export interface DesktopExit {
  code: number | null
  signal: NodeJS.Signals | null
}

/**
 * Launches Desktop in `profile` and records its window until `body` returns. The
 * session ends with Quit, which runs Desktop's own shutdown. Resolves with how
 * the app exited: the session's evidence survives a Quit that hangs.
 */
export async function withDesktopApp(
  test: TestContext,
  profile: DesktopProfile,
  body: (session: DesktopSession) => Promise<void>,
  options: { label?: string } = {},
): Promise<DesktopExit> {
  const { desktop } = inject('reel')
  if (!desktop) throw new Error('Desktop scenarios need --suite desktop')
  let app: ElectronApplication | undefined
  let exit: Promise<DesktopExit> | undefined
  await withBrowserSession(
    test,
    async (session) => {
      const { page } = session
      // The window shows a local page until Reef is ready, then Reef itself.
      await page.waitForURL((url) => url.protocol === 'http:', { timeout: 60_000 })
      const reef = new URL(page.url()).origin
      // Electron contexts take no baseURL, so resolve relative navigation
      // against the window's origin as a browser context would.
      const goto = page.goto.bind(page)
      page.goto = ((url, gotoOptions) => goto(new URL(url, reef).href, gotoOptions)) as Page['goto']
      await body({ ...session, app: app!, reef, target: { ...session.target, baseURL: reef } })
    },
    {
      label: options.label ?? 'Desktop window',
      async open({ videoDir, size }) {
        const launched = await _electron.launch({
          executablePath: desktop.executable,
          env: { ...desktop.env, ...profile.env },
          colorScheme: 'dark',
          recordVideo: { dir: videoDir, size },
          timeout: 30_000,
        })
        app = launched
        const child = launched.process()
        exit = new Promise((resolve) =>
          child.once('exit', (code, signal) => resolve({ code, signal })),
        )
        const exited = () => child.exitCode !== null || child.signalCode !== null
        try {
          const page = await launched.firstWindow()
          const video = page.video()
          return {
            context: launched.context(),
            page,
            closeContext: () => launched.close(),
            async saveVideo(path) {
              if (!video) throw new Error('The window recorded no video')
              // The app and Playwright's connection to it are gone by now,
              // but the finalized file remains.
              await copyFile(await video.path(), path)
            },
            async closeOwner() {
              if (!exited()) await forceStop(launched)
            },
            ownerClosed: exited,
          }
        } catch (error) {
          // Quit runs Desktop's own shutdown; force only what it leaves behind.
          await bounded(launched.close(), 15_000, 'Desktop quit').catch(() => {})
          if (!exited()) await forceStop(launched)
          throw error
        }
      },
    },
  )
  if (!exit) throw new Error('Desktop did not launch')
  return exit
}

/** Copies a workspace's own MCP URL from the sidebar and reads the system clipboard. */
export async function copyMcpEndpoint(page: Page, app: ElectronApplication, workspace: string) {
  const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
  await nav
    .getByRole('button', { name: `Copy ${workspace} workspace MCP URL`, exact: true })
    .click()
  // The button is named copied only after the write succeeds, so the clipboard
  // below cannot be a URL copied earlier.
  await nav
    .getByRole('button', { name: `Copy ${workspace} workspace MCP URL: copied`, exact: true })
    .waitFor()
  // Reef writes through the browser clipboard API, which Desktop's permission
  // handler must grant; read what reached the system clipboard.
  let copied = ''
  await expect
    .poll(async () => (copied = await app.evaluate(({ clipboard }) => clipboard.readText())))
    .toMatch(/^http:\/\/127\.0\.0\.1:\d+\/workspaces\/[^/]+\/mcp$/)
  return copied
}

/**
 * Replaces the system browser in Desktop's main process. Each URL Desktop
 * would open there is recorded instead; the returned function reads them.
 * Everything Desktop does up to that OS call is its production code.
 */
export async function recordSystemBrowser(app: ElectronApplication) {
  await app.evaluate(({ shell }) => {
    const opened: string[] = []
    Object.assign(globalThis, { reelOpenedExternally: opened })
    shell.openExternal = async (url: string) => void opened.push(url)
  })
  return () =>
    app.evaluate(() => [
      ...(globalThis as { reelOpenedExternally?: string[] }).reelOpenedExternally!,
    ])
}

/**
 * Answers every native question Desktop asks with the button at `response`.
 * The returned function reads the questions asked so far. Everything Desktop
 * does around the dialog is its production code.
 */
export async function answerDialogs(app: ElectronApplication, response: number) {
  await app.evaluate(({ dialog }, answer) => {
    const asked: string[] = []
    Object.assign(globalThis, { reelDialogsAsked: asked })
    dialog.showMessageBox = (async (...args: unknown[]) => {
      const options = args.at(-1) as { message: string }
      asked.push(options.message)
      return { response: answer, checkboxChecked: false }
    }) as typeof dialog.showMessageBox
  }, response)
  return () =>
    app.evaluate(() => [...(globalThis as { reelDialogsAsked?: string[] }).reelDialogsAsked!])
}

/** Closes every window, as the person does with the window's close button. */
export async function closeWindows(app: ElectronApplication) {
  await app.evaluate(({ BrowserWindow }) => {
    for (const window of BrowserWindow.getAllWindows()) window.close()
  })
}

/**
 * Launches the app again in `profile` while it runs, as a person does from
 * the launcher. The second process hands over to the first and exits.
 */
export function launchAgain(profile: DesktopProfile): Promise<DesktopExit> {
  const { desktop } = inject('reel')
  if (!desktop) throw new Error('Desktop scenarios need --suite desktop')
  // Chromium refuses to run as root with its sandbox, which the container
  // does. Playwright passes the same argument to the first launch.
  const args = process.platform === 'linux' && process.geteuid?.() === 0 ? ['--no-sandbox'] : []
  const child = spawn(desktop.executable, args, {
    env: { ...desktop.env, ...profile.env },
    stdio: 'ignore',
  })
  return bounded(
    new Promise<DesktopExit>((resolve, reject) => {
      child.once('error', reject)
      child.once('exit', (code, signal) => resolve({ code, signal }))
    }),
    30_000,
    'Second launch',
  ).finally(() => child.kill('SIGKILL'))
}

export interface DesktopProcesses {
  runtime: number[]
  reef: number[]
}

/** Finds the runtime and Reef among the app's descendants in the OS process table. */
export async function desktopProcesses(app: ElectronApplication): Promise<DesktopProcesses> {
  const owned = await descendantProcesses(app.process().pid!)
  return {
    runtime: owned
      // coral.exe on Windows.
      .filter((row) => /[/\\]resources[/\\]coral[/\\]coral\b/.test(row.args))
      .map((row) => row.pid),
    reef: owned
      .filter((row) => row.args.includes('--utility-sub-type=node.mojom.NodeService'))
      .map((row) => row.pid),
  }
}

/**
 * Kills the app and the runtime and Reef it started. Killing only Electron
 * would orphan the runtime, still writing to a data folder about to be removed.
 */
async function forceStop(app: ElectronApplication) {
  const { runtime, reef } = await desktopProcesses(app).catch(() => ({ runtime: [], reef: [] }))
  for (const pid of [app.process().pid, ...runtime, ...reef]) {
    if (pid === undefined) continue
    try {
      process.kill(pid, 'SIGKILL')
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
    }
  }
}

/** Whether a process with this id still exists. */
export function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'EPERM'
  }
}

/** Every window the app has, and how many of them the person can see. */
export function appWindows(app: ElectronApplication) {
  return app.evaluate(({ BrowserWindow }) => {
    const all = BrowserWindow.getAllWindows()
    return {
      total: all.length,
      visible: all.filter((window: { isVisible(): boolean }) => window.isVisible()).length,
    }
  })
}

/**
 * Marks the window's page, and returns a check that no document load has
 * replaced it since.
 */
export async function markSamePage(page: Page) {
  await page.evaluate(() => Object.assign(window, { reelSamePage: true }))
  return () => page.evaluate(() => 'reelSamePage' in window)
}

/**
 * Opens `url` in a separate browser that plays the person's own. The browser
 * closes with the test, even if a step hangs.
 */
export async function openInPersonsBrowser(test: TestContext, url: string): Promise<Page> {
  const browser = await chromium.launch()
  test.onTestFinished(() => browser.close().catch(() => {}))
  const tab = await browser.newPage()
  await tab.goto(url)
  return tab
}
