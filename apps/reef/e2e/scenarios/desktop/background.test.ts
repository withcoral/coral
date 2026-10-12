import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { expect, test } from 'vitest'

import { DESKTOP_DATA } from '../../support/config.ts'
import {
  answerDialogs,
  appWindows,
  closeWindows,
  desktopProcesses,
  desktopProfile,
  launchAgain,
  processExists,
  withDesktopApp,
} from '../../support/desktop-app.ts'

// Agent clients keep Coral's MCP URL, so Coral must serve with no window.
test('Coral serves after its window closes, opens again on a second launch, and stops on Quit', async (context) => {
  const profile = await desktopProfile(context)
  const desktopFile = async () =>
    JSON.parse(await readFile(join(profile.appData, DESKTOP_DATA, 'desktop.json'), 'utf8')) as {
      ports: { mcp: number; reef: number }
      toldStillRunning?: boolean
    }
  let children: number[] = []
  const quit = await withDesktopApp(context, profile, async ({ page, app, reef, step }) => {
    // The notice that Coral still runs has one button.
    const told = await answerDialogs(app, 0)
    await step('Coral runs in its window', async () => {
      await page
        .getByRole('button', { name: 'Open account menu for Local user', exact: true })
        .waitFor()
    })
    const running = await desktopProcesses(app)
    expect(running.runtime).toHaveLength(1)
    expect(running.reef).toHaveLength(1)
    children = [...running.runtime, ...running.reef]
    const { ports } = await desktopFile()
    expect((await desktopFile()).toldStillRunning).toBeUndefined()

    // The window is gone from here, so these checks have no screenshots.
    const servesWithNoWindow = async () => {
      await expect.poll(async () => (await appWindows(app)).total).toBe(0)
      expect(await desktopProcesses(app)).toEqual(running)
      // Run Status Page links name Reef's address, and agent clients the MCP one.
      for (const port of [ports.reef, ports.mcp])
        expect((await fetch(`http://127.0.0.1:${port}/readyz`)).status, `port ${port}`).toBe(200)
    }

    await closeWindows(app)
    await expect.poll(told).toHaveLength(1)
    await expect.poll(async () => (await desktopFile()).toldStillRunning).toBe(true)
    await servesWithNoWindow()

    const reopened = app.waitForEvent('window')
    // The second process found Coral running and left the window to it.
    expect(await launchAgain(profile)).toEqual({ code: 0, signal: null })
    const window = await reopened
    await window.waitForURL((url) => url.origin === reef)
    await window
      .getByRole('button', { name: 'Open account menu for Local user', exact: true })
      .waitFor()
    expect(await appWindows(app)).toEqual({ total: 1, visible: 1 })

    // Desktop says so once: the second close shows nothing.
    await closeWindows(app)
    await servesWithNoWindow()
    expect(await told()).toHaveLength(1)
    // The session ends with Quit.
  })
  // Quit finished by itself, not through the harness's fallback kill.
  expect(quit).toEqual({ code: 0, signal: null })
  for (const pid of children) expect(processExists(pid), `process ${pid}`).toBe(false)
})
