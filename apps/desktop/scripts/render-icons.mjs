// Regenerate Desktop's flat icons from Reef's favicon with electron-builder's
// own icon converter. Run `npm run icons` in apps/desktop after changing it.
import { runIconsTool } from 'app-builder-lib/out/toolsets/icons.js'
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')
const directory = join(desktopRoot, 'resources', 'icons')
const source = await readFile(join(desktopRoot, '..', 'reef', 'public', 'favicon.svg'), 'utf8')
// Reef's favicon places its 20-unit mark at 16 of the tile's 32 units, centered.
const placement = 'translate(8 8) scale(0.8)'
if (!source.includes(placement))
  throw new Error(`Reef's favicon no longer places its mark with ${placement}; update this script`)
// Enlarge the mark by 35% around the tile's center without changing the tile.
const favicon = source.replace(placement, 'translate(5.2 5.2) scale(1.08)')
const darkFavicon = favicon.replace(
  /fill="(#FFFFFF|#111111)"/g,
  (_, color) => `fill="${color === '#FFFFFF' ? '#111111' : '#FFFFFF'}"`,
)
// The macOS menu bar takes a template image: the mark alone, in black on a
// transparent ground, which macOS tints for a light or dark menu bar. The
// image is as tall as the menu bar, 24 points, and the mark fills 19 of them,
// as the system's own items do.
const trayTemplate = favicon
  .replace(/\s*<rect[^>]*\/>/, '')
  .replace('translate(5.2 5.2) scale(1.08)', 'translate(3.33 3.33) scale(1.2667)')
  .replace('fill="#111111"', 'fill="#000000"')
const macArtwork = (svg) =>
  svg
    .replace(
      /<svg[^>]*>/,
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1024 1024" fill="none">\n  <g transform="translate(112 112) scale(25)">',
    )
    .replace('</svg>', '  </g>\n</svg>')

const scratch = await mkdtemp(join(tmpdir(), 'coral-icons-'))
try {
  await writeFile(join(directory, 'icon.svg'), macArtwork(favicon))
  for (const [name, svg] of [
    ['icon', favicon],
    ['icon-dark', darkFavicon],
  ]) {
    for (const [suffix, artwork, format] of [
      ['', svg, 'ico'],
      ['-mac', macArtwork(svg), 'icns'],
    ]) {
      const output = join(scratch, `${name}${suffix}`)
      await mkdir(output)
      const inputFile = join(output, 'source.svg')
      await writeFile(inputFile, artwork)
      await runIconsTool({ inputFile, outputFormat: 'set', outDir: output })
      await runIconsTool({ inputFile, outputFormat: format, outDir: output })
      await copyFile(join(output, '512x512.png'), join(directory, `${name}${suffix}.png`))
      await copyFile(join(output, `icon.${format}`), join(directory, `${name}.${format}`))
    }
  }
  const tray = join(scratch, 'tray')
  await mkdir(tray)
  const inputFile = join(tray, 'source.svg')
  await writeFile(inputFile, trayTemplate)
  await runIconsTool({ inputFile, outputFormat: 'set', outDir: tray })
  // Electron reads the `Template` suffix, and `@2x` for Retina displays.
  await copyFile(join(tray, '24x24.png'), join(directory, 'trayTemplate.png'))
  await copyFile(join(tray, '48x48.png'), join(directory, 'trayTemplate@2x.png'))
} finally {
  await rm(scratch, { recursive: true, force: true })
}
