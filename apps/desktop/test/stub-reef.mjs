// Stands in for Reef's server in supervisor tests.
//
// Usage: stub-reef.mjs <ready|hang|exit>
//   ready  /readyz answers 200 once the runtime at CORAL_ENDPOINT answers
//   hang   /readyz answers 503 forever
//   exit   exits 5 before it listens
// GET /env answers the launch variables Reef reads. GET /exit makes it exit 9.
import { createServer } from 'node:http'

const mode = process.argv[2] ?? 'ready'
if (mode === 'exit') {
  console.error('stub reef exited during startup')
  process.exit(5)
}

// Real Reef reaches the runtime on every request, so starting it first is an error.
const runtime = await fetch(`${process.env.CORAL_ENDPOINT}/health`).catch(() => null)
if (runtime?.status !== 200) {
  console.error('stub reef started before the runtime was reachable')
  process.exit(6)
}

const names = [
  'HOST',
  'PORT',
  'CORAL_ENDPOINT',
  'CORAL_WEB_AUTH_MODE',
  'CORAL_UPSTREAM_OAUTH_CALLBACK_URL',
]
createServer((request, response) => {
  if (request.url === '/readyz') response.writeHead(mode === 'ready' ? 200 : 503).end()
  else if (request.url === '/exit') {
    response.writeHead(200).end()
    process.exit(9)
  } else if (request.url === '/env')
    response
      .writeHead(200, { 'Content-Type': 'application/json' })
      .end(JSON.stringify(Object.fromEntries(names.map((name) => [name, process.env[name]]))))
  else response.writeHead(404).end()
}).listen(Number(process.env.PORT), process.env.HOST)

process.on('SIGTERM', () => process.exit(0))
