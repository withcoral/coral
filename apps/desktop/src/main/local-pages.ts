// Reef's own mark, inlined at build time, so Desktop keeps no copy of it.
import coralMark from '../../../reef/public/app-icon.svg'

import type { ProcessExit } from './process'
import type { Failure, Role } from './supervisor'

/** Reef's shell surface, so the window matches Reef before and after it loads. */
export const SURFACE = { light: '#F0F0F0', dark: '#191919' } as const

/** The page the window shows while Coral starts. */
export function startingPage(): string {
  return page(
    'Coral is starting',
    `<main class="starting" role="status" aria-label="Coral is starting">
<div class="mark">${coralMark}</div>
<div class="spinner"></div>
</main>`,
  )
}

/**
 * The page the window shows when Coral cannot start or stops. `restarting`
 * says that Coral starts again by itself; otherwise a dialog offers Restart.
 */
export function failurePage(failure: Failure, restarting: boolean, logsDir: string): string {
  const details = 'output' in failure ? failure.output.trim() : ''
  const title = failureTitle(failure)
  const next = restarting
    ? 'Coral starts again in a few seconds.'
    : `Choose Restart to start Coral again. If the problem continues, send the logs in ${logsDir} to Coral support.`
  return page(
    title,
    `<main class="failure">
<h1>${escapeHtml(title)}</h1>
<p>${escapeHtml(failureMessage(failure))}</p>
<p>${escapeHtml(next)}</p>
${details ? `<pre>${escapeHtml(details)}</pre>` : ''}
</main>`,
  )
}

export function failureTitle(failure: Failure): string {
  return failure.kind === 'crashed' ? 'Coral stopped' : 'Coral could not start'
}

export function failureMessage(failure: Failure): string {
  switch (failure.kind) {
    case 'port-in-use':
      return `Port ${failure.port} is already in use. Another app is using it.`
    case 'exited':
      return `The ${processName(failure.process)} stopped (${exitDescription(failure.exit)}).`
    case 'crashed':
      return `The ${processName(failure.process)} stopped unexpectedly (${exitDescription(failure.exit)}).`
    case 'state-locked':
      return 'Another Coral runtime still uses Coral’s data. Wait for it to stop.'
    case 'timeout':
      return `The ${processName(failure.process)} did not become ready.`
    case 'error':
      return `Coral could not prepare to start: ${failure.message}`
  }
}

function exitDescription(exit: ProcessExit): string {
  return exit.signal ? `signal ${exit.signal}` : `exit code ${exit.code ?? 'none'}`
}

function processName(role: Role): string {
  return role === 'runtime' ? 'Coral runtime' : 'Coral web server'
}

function page(title: string, body: string): string {
  const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'">
<title>${escapeHtml(title)}</title>
<style>
:root { color-scheme: light dark; font-family: system-ui, sans-serif; background: ${SURFACE.light}; color: #202020; }
@media (prefers-color-scheme: dark) { :root { background: ${SURFACE.dark}; color: #EEEEEE; } }
body { margin: 0; line-height: 1.5; }
.starting { height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 28px; }
.mark svg { display: block; width: 64px; height: 64px; border-radius: 14px; box-shadow: 0 1px 2px rgb(0 0 0 / 0.08), 0 4px 16px rgb(0 0 0 / 0.08); }
.spinner { width: 18px; height: 18px; border-radius: 50%; border: 2px solid color-mix(in srgb, currentColor 15%, transparent); border-top-color: color-mix(in srgb, currentColor 60%, transparent); animation: spin 0.8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .spinner { animation-duration: 2.4s; } }
.failure { padding: 48px; max-width: 720px; }
.failure h1 { font-size: 20px; font-weight: 600; margin: 0 0 16px; }
pre { white-space: pre-wrap; font-size: 12px; padding: 12px; border-radius: 6px; background: color-mix(in srgb, currentColor 8%, transparent); }
</style>
</head>
<body>${body}</body>
</html>`
  return `data:text/html;charset=utf-8,${encodeURIComponent(html)}`
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (char) => `&#${char.charCodeAt(0)};`)
}
