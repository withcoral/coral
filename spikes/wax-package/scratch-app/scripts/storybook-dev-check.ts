// SPIKE (withcoral/coral#3541). Starts `storybook dev`, opens Wax stories that the
// package ships in Chromium, and reports page errors and whether Wax's styles applied.
// Storybook's dev server pre-bundles dependencies as Vite's does, so it is a separate
// check from `storybook build`.
//
//   node scripts/storybook-dev-check.ts [port]
import { spawn } from 'node:child_process'

import { chromium } from 'playwright'

const port = Number(process.argv[2] ?? 6111)
const origin = `http://localhost:${port}`
const storybook = spawn('npx', ['storybook', 'dev', '--ci', '--port', String(port)], {
  detached: true,
  env: { ...process.env, STORYBOOK_DISABLE_TELEMETRY: '1' },
  stdio: 'ignore',
})
const failures: string[] = []
try {
  let index: { entries: Record<string, { id: string; importPath: string; type: string }> } | null =
    null
  for (let attempt = 0; attempt < 120 && !index; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 500))
    index = await fetch(`${origin}/index.json`)
      .then((response) => (response.ok ? response.json() : null))
      .catch(() => null)
  }
  if (!index) throw new Error('storybook dev did not start')
  const stories = Object.values(index.entries).filter((entry) => entry.type === 'story')
  const fromPackage = stories.filter((entry) => entry.importPath.includes('@withcoral/wax'))
  // One story from each of a few components: a recipe, Base UI, lucide, react-aria.
  const wanted = ['button', 'dialog', 'icon', 'date-picker', 'table', 'payload-viewer']
  const opened = wanted.flatMap((name) => {
    const found = fromPackage.find((entry) => entry.importPath.includes(`/components/${name}/`))
    return found ? [found] : []
  })
  const browser = await chromium.launch()
  for (const story of opened) {
    const page = await browser.newPage()
    const before = failures.length
    page.on('pageerror', (error) => failures.push(`${story.id}: page error: ${error.message}`))
    await page.goto(`${origin}/iframe.html?id=${story.id}&viewMode=story`, {
      waitUntil: 'networkidle',
    })
    const seen = await page.evaluate(async () => {
      // A cold server reloads the page once when it finds a new dependency.
      for (let wait = 0; wait < 100; wait++) {
        if (document.querySelector('#storybook-root')?.childElementCount) break
        await new Promise((resolve) => setTimeout(resolve, 100))
      }
      const root = document.querySelector('#storybook-root')
      const error = document.querySelector('.sb-show-errordisplay #error-message')?.textContent
      const classes = [...(root?.querySelectorAll('[class]') ?? [])].flatMap((e) => [...e.classList])
      return {
        children: root?.childElementCount ?? 0,
        error: error ?? null,
        // A Vanilla Extract class has a debug name and a hash, such as `button__zr0k795`.
        waxClasses: classes.filter((name) => /__[a-z0-9]{6,}$/.test(name)).length,
        bodyFont: getComputedStyle(document.body).fontFamily.split(',')[0],
      }
    })
    console.log(story.id, JSON.stringify(seen))
    if (seen.error) failures.push(`${story.id}: ${seen.error.slice(0, 300)}`)
    if (!seen.children) failures.push(`${story.id}: rendered nothing`)
    if (!seen.waxClasses) failures.push(`${story.id}: no Wax class`)
    if (failures.length === before && !seen.bodyFont.includes('Encode Sans')) {
      failures.push(`${story.id}: body font is ${seen.bodyFont}`)
    }
    await page.close()
  }
  await browser.close()
  console.log(`stories in the index: ${stories.length}, from the package: ${fromPackage.length}`)
  if (opened.length !== wanted.length) failures.push(`opened ${opened.length} of ${wanted.length}`)
} catch (error) {
  failures.push(`threw: ${error instanceof Error ? error.message : String(error)}`)
} finally {
  if (storybook.pid) process.kill(-storybook.pid)
}
if (failures.length) {
  console.error(`FAILED\n${failures.join('\n')}`)
  process.exit(1)
}
console.log('OK')
