// SPIKE (withcoral/coral#3541). Starts the Vite dev server, opens the page in Chromium
// and reports what a person would see: page errors, and whether Wax's styles applied.
// Then loads the server entry through Vite's SSR module loader, as React Router does.
import { chromium } from 'playwright'
import { createServer } from 'vite'

const server = await createServer({ server: { port: 5199, strictPort: true }, logLevel: 'warn' })
await server.listen()
const failures: string[] = []
try {
  const browser = await chromium.launch()
  const page = await browser.newPage()
  page.on('pageerror', (error) => failures.push(`page error: ${error.message}`))
  page.on('console', (message) => {
    if (message.type() === 'error') failures.push(`console error: ${message.text()}`)
  })
  page.on('requestfailed', (request) => failures.push(`request failed: ${request.url()}`))
  await page.goto('http://localhost:5199/', { waitUntil: 'networkidle' })
  const seen = await page.evaluate(async () => {
    const button = document.querySelector('button')
    const pageBox = document.querySelector('[data-testid="page"]')
    await document.fonts.ready
    // A button with no class, to compare with: Chromium paints one by default.
    const plain = document.body.appendChild(document.createElement('button'))
    const plainBackground = getComputedStyle(plain).backgroundColor
    plain.remove()
    return {
      plainBackground,
      button: button && {
        className: button.className,
        borderRadius: getComputedStyle(button).borderRadius,
        backgroundColor: getComputedStyle(button).backgroundColor,
      },
      bodyFont: getComputedStyle(document.body).fontFamily,
      fontsLoaded: [...document.fonts].filter((font) => font.status === 'loaded').map((f) => f.family),
      pageBackground: pageBox && getComputedStyle(pageBox).backgroundColor,
    }
  })
  console.log('browser', JSON.stringify(seen, null, 2))
  if (!seen.button) failures.push('no button rendered')
  if (seen.button?.backgroundColor === seen.plainBackground) {
    failures.push('button has no Wax style')
  }
  if (!seen.fontsLoaded.length) failures.push('no font loaded')
  await browser.close()

  const entry = await server.ssrLoadModule('/src/entry-server.tsx')
  const rendered = entry.render()
  console.log('ssr', JSON.stringify({ font: rendered.font, html: rendered.html.slice(0, 160) }))
  if (!rendered.html.includes('<button')) failures.push('SSR rendered no button')
} catch (error) {
  failures.push(`threw: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`)
} finally {
  await server.close()
}
if (failures.length) {
  console.error(`FAILED\n${failures.join('\n')}`)
  process.exit(1)
}
console.log('OK')
