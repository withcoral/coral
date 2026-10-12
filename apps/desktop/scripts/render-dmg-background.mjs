// Renders resources/dmg/background.svg into the PNGs the dmg window shows:
// background@2x.png at 1280 × 840 for Retina displays, and background.png at
// 640 × 420. It uses Reef's own fonts. Run it again after changing the SVG:
//
//   npx electron scripts/render-dmg-background.mjs
import { app, BrowserWindow } from 'electron'
import { readFileSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

const WIDTH = 640
const HEIGHT = 420
const desktopRoot = resolve(import.meta.dirname, '..')
const directory = join(desktopRoot, 'resources', 'dmg')
const fontFace = (weight) => {
  const font = readFileSync(
    join(
      desktopRoot,
      '..',
      'reef',
      'public',
      'fonts',
      'encode-sans-semi-expanded',
      `EncodeSansSemiExpanded-${weight}-latin.woff2`,
    ),
  ).toString('base64')
  return `@font-face { font-family: 'Encode Sans Semi Expanded'; font-weight: ${weight}; src: url(data:font/woff2;base64,${font}); }`
}
const page = `<!doctype html>
<style>
${fontFace(500)}
${fontFace(700)}
html, body { margin: 0; overflow: hidden; }
svg { display: block; width: 100vw; height: 100vh; }
</style>
${readFileSync(join(directory, 'background.svg'), 'utf8')}`

// Draw at Retina density whatever the screen, then scale down for 1x.
app.commandLine.appendSwitch('force-device-scale-factor', '2')
app.disableHardwareAcceleration()
// A top-level await here would hold Electron's startup, so ready never fires.
void app
  .whenReady()
  .then(render)
  .then(
    () => app.quit(),
    (error) => {
      console.error(error)
      app.exit(1)
    },
  )

async function render() {
  const window = new BrowserWindow({
    show: false,
    width: WIDTH,
    height: HEIGHT,
    useContentSize: true,
    webPreferences: { offscreen: true },
  })
  await window.loadURL(`data:text/html;charset=utf-8,${encodeURIComponent(page)}`)
  await window.webContents.executeJavaScript('document.fonts.ready.then(() => true)')
  // Let the offscreen frame paint with the loaded font before capturing it.
  await new Promise((settle) => setTimeout(settle, 500))
  const retina = await window.webContents.capturePage()
  const { width, height } = retina.getSize()
  if (width !== WIDTH * 2 || height !== HEIGHT * 2)
    throw new Error(`The background rendered at ${width} × ${height}`)
  writeFileSync(join(directory, 'background@2x.png'), retina.toPNG())
  writeFileSync(
    join(directory, 'background.png'),
    retina.resize({ width: WIDTH, height: HEIGHT, quality: 'best' }).toPNG(),
  )
  window.destroy()
}
