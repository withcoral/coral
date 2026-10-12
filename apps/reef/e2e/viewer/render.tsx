import type { ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'

import { LandingPage, RunPage } from './run-pages'
import { ScenarioPage } from './scenario-page'
import { SessionPage } from './session-page'
import { RecordedFrame, sessionLabel } from './shared'
import type { RunSummary, ScenarioPageProps, SessionPageProps } from './types'
import '../../app/wax/theme/reset.css'
import '../../app/wax/theme/global.css'
import '../../app/wax/theme/theme-system.css'

function document(title: string, body: ReactNode) {
  return (
    '<!doctype html>' +
    renderToStaticMarkup(
      <html lang="en">
        <head>
          <meta charSet="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>{title === 'Reel' ? title : `${title} · Reel`}</title>
          <link
            rel="icon"
            type="image/svg+xml"
            href={`data:image/svg+xml,${encodeURIComponent(
              '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🍿</text></svg>',
            )}`}
          />
        </head>
        <body>
          <RecordedFrame>{body}</RecordedFrame>
        </body>
      </html>,
    )
  )
}
export function session(props: SessionPageProps) {
  return document(sessionLabel(props.result), <SessionPage {...props} />)
}
export function scenario(props: ScenarioPageProps) {
  return document(props.result.scenario, <ScenarioPage {...props} />)
}
export function run(data: RunSummary) {
  return document('Test run', <RunPage run={data} />)
}
export function landing(runs: RunSummary[]) {
  return document('Reel', <LandingPage runs={runs} />)
}
