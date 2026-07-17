import { readFile } from 'node:fs/promises'

const marker = '[coral-updater] release updater enabled'
const bundlePath = new URL('../out/main/index.js', import.meta.url)
const bundle = await readFile(bundlePath, 'utf8')

if (!bundle.includes(marker)) {
  throw new Error(
    'desktop release bundle does not contain the updater; build it with CORAL_DESKTOP_RELEASE=1',
  )
}

console.info('Verified updater in desktop release bundle')
