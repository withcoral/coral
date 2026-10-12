import { buildStaticRenderer } from '../../scripts/static-renderer.ts'
import { staticDocument } from '../../scripts/static-document.ts'
import { playbackScript } from './playback.ts'

async function buildViewer() {
  const { css, render } = await buildStaticRenderer<typeof import('../viewer/render')>(
    new URL('../viewer/render.tsx', import.meta.url),
  )
  const styled =
    <Args extends unknown[]>(view: (...args: Args) => string) =>
    (...args: Args) =>
      staticDocument(view(...args), css)
  return {
    render: {
      session: (...args: Parameters<typeof render.session>) => {
        const page = styled(render.session)(...args)
        if (args[0].result.recording) {
          page.html = page.html.replace('</body>', `<script>${playbackScript}</script></body>`)
        }
        return page
      },
      scenario: styled(render.scenario),
      run: styled(render.run),
      landing: styled(render.landing),
    },
  }
}
let renderer: ReturnType<typeof buildViewer> | undefined

// One trusted, server-only Wax bundle per process. No Reef browser runtime:
// artifacts get a stylesheet snapshot and recordings get the playback enhancement.
export function viewerRenderer() {
  return (renderer ??= buildViewer())
}
