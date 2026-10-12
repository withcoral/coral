import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import { build } from 'vite'

const root = fileURLToPath(new URL('..', import.meta.url))

/** Compile trusted React/Wax views for Node without shipping browser JavaScript. */
export async function buildStaticRenderer<T>(entry: URL): Promise<{ css: string; render: T }> {
  const result = await build({
    configFile: false,
    root,
    logLevel: 'silent',
    resolve: { alias: { '@': `${root}/app` } },
    plugins: [vanillaExtractPlugin()],
    ssr: { noExternal: true },
    build: {
      ssr: fileURLToPath(entry),
      ssrEmitAssets: true,
      write: false,
      cssCodeSplit: false,
      rollupOptions: { output: { codeSplitting: false } },
    },
  })
  const bundle = Array.isArray(result) ? result[0] : result
  if (!('output' in bundle)) throw new Error('Static renderer did not produce a bundle')
  const script = bundle.output.find((file) => file.type === 'chunk' && file.isEntry)
  const styles = bundle.output.filter(
    (file) => file.type === 'asset' && file.fileName.endsWith('.css'),
  )
  if (!script || script.type !== 'chunk' || !styles.length)
    throw new Error('Missing static renderer or styles')
  let css = styles
    .map((file) => (file.type === 'asset' ? Buffer.from(file.source).toString('utf8') : ''))
    .join('\n')
  // Self-contained fonts work on the auth listener and in private evidence
  // viewers without a dependency on Reef's static-file listener.
  for (const match of css.matchAll(/url\(["']?(\/fonts\/[\w./-]+\.woff2)["']?\)/g)) {
    const font = await readFile(`${root}/public${match[1]}`)
    css = css.replaceAll(match[0], `url(data:font/woff2;base64,${font.toString('base64')})`)
  }
  // The vanilla-extract plugin keeps `@vanilla-extract/css` external for SSR,
  // so import the bundle from where Node resolves Reef's packages.
  const temporary = await mkdtemp(join(root, 'node_modules', '.coral-static-renderer-'))
  try {
    const file = join(temporary, 'render.mjs')
    await writeFile(file, script.code)
    const render = (await import(pathToFileURL(file).href)) as T
    return { css, render }
  } finally {
    await rm(temporary, { recursive: true, force: true })
  }
}
