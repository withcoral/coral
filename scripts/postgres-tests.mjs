import { spawn } from 'node:child_process'
import { LocalPostgres } from './postgres.ts'

const env = { ...process.env }
let database
let child
let interrupted = false
let timer
const stop = (signal) => {
  if (!child?.pid) return
  try {
    process.kill(-child.pid, signal)
  } catch (error) {
    if (error.code !== 'ESRCH') throw error
  }
}
const interrupt = () => {
  interrupted = true
  stop('SIGTERM')
  timer ??= setTimeout(() => stop('SIGKILL'), 5000)
}
process.on('SIGINT', interrupt)
process.on('SIGTERM', interrupt)
try {
  database = await new LocalPostgres(env).provision(env.CORAL_TEST_POSTGRES_URL)
  if (interrupted) throw new Error('Postgres tests interrupted')
  const [command, ...args] = process.argv.slice(2)
  if (!command) throw new Error('A repository test command is required')
  child = spawn(command, args, {
    env: { ...env, CORAL_TEST_DATABASE: 'postgres', CORAL_TEST_POSTGRES_URL: database.url },
    stdio: 'inherit',
    detached: true,
  })
  process.exitCode = await new Promise((resolve, reject) => {
    child.once('error', reject)
    child.once('exit', (code) => resolve(code ?? 1))
  })
} catch (error) {
  console.error(error.message)
  process.exitCode = 1
} finally {
  clearTimeout(timer)
  stop('SIGKILL')
  try {
    await database?.close()
  } catch (error) {
    console.error(error.message)
    process.exitCode = 1
  }
  process.off('SIGINT', interrupt)
  process.off('SIGTERM', interrupt)
}
