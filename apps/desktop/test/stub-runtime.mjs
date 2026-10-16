// Stands in for the `coral` runtime in supervisor tests. It reads the binds from
// the config.toml Desktop wrote, so it also proves the file reaches the runtime.
//
// Usage: stub-runtime.mjs <ready|hang|fail|leave|stubborn>
//   ready     MCP /readyz answers 200
//   hang      MCP /readyz answers 503 forever
//   fail      refuses to start and exits 3
//   leave     like ready, but exits 8 once Reef has reached it, before Reef is ready
//   stubborn  like ready, but ignores its stdin closing
// Like the runtime with CORAL_STOP_ON_STDIN_CLOSE=1, it exits 0 when its stdin
// closes, and it does not handle SIGTERM. Files in its state directory change
// a launch, so a test can change later launches:
//   stub.lock    exit 75, as the runtime does when another one holds the directory
//   stub.refuse  like fail
//   stub.hang    like hang
// GET /exit on the MCP listener makes a running stub exit 7. GET /launch answers
// with the config.toml and the database URL variable that the stub started with.
import { existsSync, readFileSync } from 'node:fs'
import { createServer } from 'node:http'
import { join } from 'node:path'

const marked = (name) => existsSync(join(process.env.CORAL_CONFIG_DIR, name))
const mode = marked('stub.refuse')
  ? 'fail'
  : marked('stub.hang')
    ? 'hang'
    : (process.argv[2] ?? 'ready')
if (mode === 'fail') {
  console.error('stub runtime refused to start')
  process.exit(3)
}
if (marked('stub.lock')) {
  console.error('stub runtime found its state directory locked')
  process.exit(75)
}
if (process.env.CORAL_STOP_ON_STDIN_CLOSE === '1' && mode !== 'stubborn') {
  process.stdin.on('end', () => process.exit(0))
  process.stdin.resume()
}

const config = readFileSync(join(process.env.CORAL_CONFIG_DIR, 'config.toml'), 'utf8')
const bind = (listener) => {
  const match = config.match(
    new RegExp(`\\[listeners\\.${listener}\\]\\nbind = "127\\.0\\.0\\.1:(\\d+)"`),
  )
  if (!match) throw new Error(`config.toml has no ${listener} bind`)
  return Number(match[1])
}

// The gRPC listener's stand-in: stub Reef calls it before it reports ready.
const grpc = createServer((request, response) => {
  response.writeHead(request.url === '/health' ? 200 : 404).end()
  if (request.url === '/health' && mode === 'leave') response.on('finish', () => process.exit(8))
})
const mcp = createServer((request, response) => {
  if (request.url === '/readyz') response.writeHead(mode === 'hang' ? 503 : 200).end()
  else if (request.url === '/launch')
    response.writeHead(200, { 'content-type': 'application/json' }).end(
      JSON.stringify({
        config,
        env: { CORAL_DESKTOP_DATABASE_URL: process.env.CORAL_DESKTOP_DATABASE_URL },
      }),
    )
  else if (request.url === '/exit') {
    response.writeHead(200).end()
    process.exit(7)
  } else response.writeHead(404).end()
})
// Like the real runtime, take a moment to start, so a supervisor that does not
// wait for readiness starts Reef too early.
setTimeout(() => {
  grpc.listen(bind('grpc'), '127.0.0.1')
  mcp.listen(bind('mcp'), '127.0.0.1')
  console.log('stub runtime listening')
}, 300)

process.on('SIGTERM', () => {})
