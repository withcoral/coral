import { spawn } from 'node:child_process'
import { readFileSync } from 'node:fs'

// Launch only after the runner records this command's process group and
// installs its interruption handlers. The runner sends "start" through an
// extra pipe (file descriptor 3).
// If the runner exits before sending it, exit without launching the command.
if (readFileSync(3, 'utf8') !== 'start') process.exit(1)
const [command, ...args] = process.argv.slice(2)
const child = spawn(command, args, { stdio: 'inherit' })
child.once('error', () => {
  process.exitCode = 1
})
child.once('exit', (code) => {
  process.exitCode = code ?? 1
})
