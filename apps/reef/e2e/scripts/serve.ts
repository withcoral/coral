import { mkdir } from 'node:fs/promises'
import { runsRoot } from '../support/config.ts'
import { resultsServer } from '../support/results-server.ts'

const port = Number(process.env.E2E_RESULTS_PORT ?? 9324)
if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('Invalid E2E_RESULTS_PORT')
const externalOrigin = process.env.E2E_RESULTS_ORIGIN
await mkdir(runsRoot, { recursive: true })
const server = await resultsServer(runsRoot, port, externalOrigin)
console.log(`Reel: http://127.0.0.1:${(server.address() as { port: number }).port}/`)
if (externalOrigin !== undefined) {
  console.log(`Configured viewer origin: ${externalOrigin}`)
  console.warn(
    'Sensitive evidence: this viewer has no authentication. With Tailscale Serve, anyone permitted by your tailnet policy can read it. Host checks are not authentication. Do not use Funnel or publish it publicly.',
  )
  console.log('The viewer still binds to loopback. Configure Tailscale Serve separately.')
} else {
  console.log(
    'Sensitive local evidence: localhost only. Remote viewing requires E2E_RESULTS_ORIGIN.',
  )
}
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    server.close()
    server.closeAllConnections()
  })
}
