import { randomUUID } from 'node:crypto'
import { mkdir, rm } from 'node:fs/promises'
import { join, relative } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import { chromium, type BrowserContext, type BrowserContextOptions, type Page } from 'playwright'
import { inject, type TestContext } from 'vitest'

import { exec, renderSession, slug, snapshotSource, type Result } from './artifacts.ts'
import { reefRoot, type Target } from './config.ts'
import { bounded } from './stack.ts'

export interface Session {
  page: Page
  target: Target
  step: (label: string, action: () => Promise<void>) => Promise<void>
}

/** The recorded page and what owns it: a fresh Chromium context, or a Desktop window. */
export interface SessionSurface {
  context: BrowserContext
  page: Page
  /** Ends the context, which finalizes its video. */
  closeContext(): Promise<void>
  /** Saves the finalized video. */
  saveVideo(path: string): Promise<void>
  /** Closes whatever launched the context. Safe to call again. */
  closeOwner(): Promise<void>
  /** Whether that owner is gone, so no browser or app outlives the session. */
  ownerClosed(): boolean
}

export type OpenSurface = (recording: {
  baseURL: string
  videoDir: string
  size: { width: number; height: number }
  film: boolean
}) => Promise<SessionSurface>

const sessionSequences = new WeakMap<TestContext['task'], number>()

/**
 * Makes `goto` and `reload` return only once React has hydrated the page they
 * load. Reef's server markup shows first, and a key or click that lands before
 * hydration does nothing. Pages that React Router did not render, such as
 * fixture pages and resource routes, return at once.
 */
function waitForHydrationOnLoad(page: Page) {
  const hydrated = () =>
    page.waitForFunction(
      () => !('__reactRouterContext' in window) || 'hydrated' in document.documentElement.dataset,
    )
  const goto = page.goto.bind(page)
  page.goto = (async (url, options) => {
    const response = await goto(url, options)
    await hydrated()
    return response
  }) as Page['goto']
  const reload = page.reload.bind(page)
  page.reload = (async (options) => {
    const response = await reload(options)
    await hydrated()
    return response
  }) as Page['reload']
}

// Tests retain raw Playwright access. The only abstractions are resource/evidence
// ownership and the hydration wait; no product workflow or authorization rules
// live here.
export async function withBrowserSession(
  test: TestContext,
  body: (session: Session) => Promise<void>,
  options: {
    target?: Target
    storageState?: BrowserContextOptions['storageState']
    actor?: string
    label?: string
    purpose?: 'setup' | 'interaction'
    /** Records somewhere other than a fresh headless Chromium. */
    open?: OpenSurface
  } = {},
) {
  const shared = inject('reel')
  const config = { ...shared, target: options.target ?? shared.target }
  const ffmpegBinary = config.ffmpegBinary
  const started = Date.now()
  const source = test.task.file.filepath
  const id = randomUUID()
  const sequence = (sessionSequences.get(test.task) ?? 0) + 1
  sessionSequences.set(test.task, sequence)
  const dir = join(config.runDir, config.target.name, test.task.id, 'sessions', id)
  const result: Result = {
    id,
    scenarioId: test.task.id,
    actor: options.actor,
    label: options.label ?? 'Reef interaction',
    purpose: options.purpose ?? 'interaction',
    sequence,
    attempt: 1 + (test.task.result?.retryCount ?? 0) + (test.task.result?.repeatCount ?? 0),
    scenario: test.task.name,
    source: relative(reefRoot, source),
    target: config.target.name,
    status: 'passed',
    startedAt: new Date(started).toISOString(),
    durationMs: 0,
    steps: [],
    warnings: [],
    browserClosed: false,
  }
  let surface: SessionSurface | undefined
  let page: Page | undefined
  let failure: unknown
  let finishing: Promise<void> | undefined

  const attempt = async (label: string, action: () => Promise<unknown>, ms = 10_000) => {
    try {
      await bounded(action(), ms, label)
      return true
    } catch (error) {
      const warning = `${label}: ${String(error)}`
      result.warnings.push(warning)
      console.warn(warning)
      return false
    }
  }
  const finish = (error?: string, skipped = false) => {
    finishing ??= (async () => {
      result.durationMs = Date.now() - started
      if (skipped) {
        result.status = 'skipped'
      } else if (error || failure || test.signal.aborted) {
        result.status = 'failed'
        result.failure = error || String(failure || test.signal.reason || 'Test aborted')
        if (page)
          await attempt(
            'Failure screenshot',
            () => page!.screenshot({ path: join(dir, 'failure.png'), timeout: 5000 }),
            6000,
          )
      }
      if (surface) {
        await attempt('Trace', async () => {
          await surface!.context.tracing.stop({ path: join(dir, 'trace.zip') })
          result.trace = 'trace.zip'
        })
        await attempt('Context close', () => surface!.closeContext())
        await attempt('Video', async () => {
          await surface!.saveVideo(join(dir, 'session.webm'))
          result.recording = 'session.webm'
          // Remove Playwright scratch files only after a durable WebM exists.
          await rm(join(dir, '.video'), { recursive: true, force: true })
        })
        await attempt('Browser close', () => surface!.closeOwner())
      }
      result.browserClosed = surface?.ownerClosed() ?? true
      if (result.recording && ffmpegBinary)
        await attempt(
          'Optional MP4 conversion (WebM retained)',
          async () => {
            try {
              await exec(
                ffmpegBinary,
                [
                  '-y',
                  '-i',
                  join(dir, 'session.webm'),
                  '-c:v',
                  'libx264',
                  '-preset',
                  'veryfast',
                  '-crf',
                  '26',
                  '-pix_fmt',
                  'yuv420p',
                  '-movflags',
                  '+faststart',
                  join(dir, 'session.mp4'),
                ],
                { timeout: 20_000, killSignal: 'SIGKILL' },
              )
              result.recording = 'session.mp4'
            } catch (conversionError) {
              await rm(join(dir, 'session.mp4'), { force: true })
              throw conversionError
            }
          },
          22_000,
        )
      const originalFailed = result.status === 'failed'
      const missingEvidence = !result.recording || !result.trace || !result.browserClosed
      if (result.status === 'passed' && missingEvidence) {
        result.status = 'failed'
        result.failure = 'Recording, trace, or browser cleanup failed; see artifact warnings'
      }
      const saved = await attempt('Result metadata and viewer', () => renderSession(dir, result))
      // Only optional transcoding is allowed to degrade a green recorded test.
      // Never replace a product assertion or timeout with an artifact error.
      if (!originalFailed && !skipped && (missingEvidence || !saved)) {
        throw new Error(result.failure || 'Could not save Reel metadata')
      }
    })()
    return finishing
  }

  // Unlike a bare finally, this hook runs even when Vitest times out a callback
  // waiting on an arbitrary, uncancellable promise. Finalization is idempotent.
  test.onTestFinished(async ({ task }) => {
    const errors = task.result?.errors?.map((error) => error.stack || error.message).join('\n')
    if (!finishing) {
      await finish(errors, task.result?.state === 'skip')
    } else {
      // A completed recording keeps its own outcome. The reporter captures
      // later assertion/cleanup failures on the parent scenario.
      try {
        await finishing
      } catch (error) {
        if (!errors) throw error
      }
    }
  }, 90_000)

  const active = () => {
    test.signal.throwIfAborted()
    if (finishing) throw new Error('Browser session already finalized')
  }
  try {
    await mkdir(dir, { recursive: true })
    await snapshotSource(source, join(dir, 'scenario.ts'))
    active()
    const open = options.open ?? openChromium(options.storageState)
    surface = await open({
      baseURL: config.target.baseURL,
      videoDir: join(dir, '.video'),
      size: { width: 1280, height: 800 },
      film: config.film,
    })
    const { context } = surface
    page = surface.page
    waitForHydrationOnLoad(page)
    context.on('page', waitForHydrationOnLoad)
    active()
    context.setDefaultTimeout(5000)
    context.setDefaultNavigationTimeout(10_000)
    await context.tracing.start({ screenshots: true, snapshots: true, sources: true })
    active()
    await body({
      page,
      target: config.target,
      async step(label, action) {
        active()
        await context.tracing.group(label)
        try {
          await action()
        } finally {
          if (!finishing) await attempt('Trace group close', () => context.tracing.groupEnd())
        }
        active()
        const screenshot = `${String(result.steps.length).padStart(2, '0')}-${slug(label)}.png`
        await page!.screenshot({ path: join(dir, screenshot), timeout: 5000 })
        result.steps.push({ label, screenshot, elapsedMs: Date.now() - started })
        if (config.film) await delay(1500, undefined, { signal: test.signal })
      },
    })
  } catch (error) {
    failure = error
    // Launch/context creation can finish after timeout teardown began. Close any
    // late-arriving resources as well, without replacing the original failure.
    if (finishing && surface) {
      await attempt('Late context close', () => surface!.closeContext())
      await attempt('Late browser close', () => surface!.closeOwner())
    }
    throw error
  } finally {
    // Normal completion closes now; the test-finish hook remains the fallback
    // for a timeout on an arbitrary promise that never reaches this finally.
    if (!test.signal.aborted && !finishing) await finish()
  }
}

function openChromium(storageState: BrowserContextOptions['storageState']): OpenSurface {
  return async ({ baseURL, videoDir, size, film }) => {
    const browser = await chromium.launch({
      headless: true,
      slowMo: film ? 400 : 0,
      timeout: 15_000,
    })
    try {
      const context = await browser.newContext({
        baseURL,
        viewport: size,
        colorScheme: 'dark',
        storageState,
        recordVideo: { dir: videoDir, size },
      })
      const page = await context.newPage()
      const video = page.video()
      return {
        context,
        page,
        closeContext: () => context.close(),
        // saveAs still needs Playwright's connection, even after context.close
        // finalized the recording. Save before disconnecting the browser.
        async saveVideo(path) {
          if (!video) throw new Error('The context recorded no video')
          await video.saveAs(path)
        },
        closeOwner: () => browser.close(),
        ownerClosed: () => !browser.isConnected(),
      }
    } catch (error) {
      await browser.close().catch(() => {})
      throw error
    }
  }
}
