import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath, pathToFileURL } from 'node:url'

import { buildStaticRenderer } from './static-renderer.ts'

const reset = fileURLToPath(new URL('../app/wax/theme/reset.css', import.meta.url))

/** An entry that renders nothing and imports one stylesheet. */
async function entryWith(t: { after(cleanup: () => Promise<void>): void }, stylesheet: string) {
  const folder = await mkdtemp(join(tmpdir(), 'reef-static-'))
  t.after(() => rm(folder, { recursive: true, force: true }))
  await writeFile(join(folder, 'styles.css'), stylesheet)
  await writeFile(join(folder, 'entry.ts'), "import './styles.css'\nexport const render = ''\n")
  return pathToFileURL(join(folder, 'entry.ts'))
}

// Catches: a page on the auth listener or in a Reel viewer that asks a server for a font.
test('static styles carry each Wax font inside them', async (t) => {
  const entry = await entryWith(t, `@import ${JSON.stringify(reset)};\n`)
  const { css } = await buildStaticRenderer(entry)

  assert.equal(css.match(/url\(["']?data:font\/woff2;base64,/g)?.length, 10)
  assert.doesNotMatch(css, /\.woff2/)
})

test('a font that the build does not emit fails the build', async (t) => {
  const entry = await entryWith(
    t,
    "@font-face { font-family: Elsewhere; src: url('/fonts/elsewhere.woff2') format('woff2'); }\n",
  )

  await assert.rejects(buildStaticRenderer(entry), /font the build did not emit/)
})
