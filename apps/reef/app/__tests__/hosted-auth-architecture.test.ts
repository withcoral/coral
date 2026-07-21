/**
 * Structural invariants for hosted authentication.
 *
 * These are deliberately separate from `architecture.test.ts`, which covers
 * component and styling conventions. Each case here pins a specific regression
 * this stack fixed, and exists because the defect it guards was invisible to
 * every other kind of test — a marker nothing sets, a route outside the auth
 * boundary, a client built without a token. Per AGENTS.md, Reef keeps
 * architectural invariants only where they protect a named contract; the named
 * contract is recorded on each case below.
 */

import * as fs from 'node:fs'
import * as path from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const appDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const routeConfigFile = path.join(appDir, 'routes.ts')
const reefRoot = path.resolve(appDir, '..')
const desktopRoot = path.resolve(reefRoot, '..', 'desktop')

function read(...segments: string[]): string {
  return fs.readFileSync(path.join(...segments), 'utf8')
}

function leadingIndentWidth(line: string): number {
  return (/^\s*/.exec(line)?.[0] ?? '').length
}

describe('hosted auth architecture', () => {
  // Regression: `reefAuthConfig` gated the Desktop bypass on
  // `import.meta.env.VITE_CORAL_DESKTOP_APP`, a variable nothing in the repo
  // sets, so the branch was dead.
  //
  // Scoped to the four places that *define* the marker, because those are what
  // no test executes — a Vite config and two Desktop build scripts. How the auth
  // config reads it is covered for real in `config.server.test.ts`, which mocks
  // the helper: any consumer that bypasses it, however spelled, fails there.
  it('derives Desktop behavior from one build marker, read through one helper', () => {
    const viteConfig = read(reefRoot, 'vite.config.ts')
    const desktopHelper = read(appDir, 'lib', 'coral-desktop.ts')
    const devScript = read(desktopRoot, 'scripts', 'dev.mjs')
    const stageScript = read(desktopRoot, 'scripts', 'stage-coral.mjs')

    for (const source of [viteConfig, desktopHelper, devScript, stageScript]) {
      expect(source).not.toContain('VITE_CORAL_DESKTOP_APP')
    }
    expect(viteConfig).toMatch(
      /'import\.meta\.env\.CORAL_DESKTOP_APP':\s*JSON\.stringify\(\s*process\.env\.CORAL_DESKTOP_APP === '1',?\s*\)/,
    )
    expect(desktopHelper).toMatch(/return import\.meta\.env\.CORAL_DESKTOP_APP/)
    expect(devScript.match(/CORAL_DESKTOP_APP:\s*'1'/g)).toHaveLength(1)
    expect(stageScript.match(/CORAL_DESKTOP_APP:\s*'1'/g)).toHaveLength(1)
  })

  // Regression: a route added at the top level of `routes.ts` sits outside the
  // `_protected` boundary and is served with no session check. Comparing source
  // positions would not catch it — a route appended after the boundary block
  // closes still compares "later" — so this scans the top-level entries
  // themselves, and every public one has to be named here on purpose.
  //
  // Depth is read from the file rather than assumed: pinning a literal
  // two-space indent would make an auth guard only as strong as the formatter,
  // and a reindent would drop entries out of the scan silently instead of
  // failing.
  it('keeps every route inside the auth boundary except the public ones named here', () => {
    const routeConfig = read(routeConfigFile)
    const publicTopLevelRoutes = [
      "route('.well-known/oauth-client', 'routes/oauth-client-metadata.ts'),",
      "route(routePattern('login'), 'routes/login.tsx'),",
      "route('auth/callback', 'routes/auth.callback.tsx'),",
      "route('logout', 'routes/logout.tsx'),",
    ]

    const entryLines = routeConfig
      .slice(routeConfig.indexOf('export default ['))
      .split('\n')
      .filter((line) => /^\s+(?:index|layout|route)\(/.test(line))
    const topLevelIndent = Math.min(...entryLines.map(leadingIndentWidth))
    const topLevelEntries = entryLines
      .filter((line) => leadingIndentWidth(line) === topLevelIndent)
      .map((line) => line.trim())

    // Guards the guard: an empty scan would make everything below it vacuous.
    expect(entryLines.length).toBeGreaterThan(0)
    expect(topLevelEntries).toContain("layout('routes/_protected.tsx', [")
    expect(
      topLevelEntries.filter(
        (entry) =>
          !publicTopLevelRoutes.includes(entry) &&
          !entry.startsWith("layout('routes/_protected.tsx',"),
      ),
    ).toEqual([])
  })
})
