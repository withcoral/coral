import { LocalPostgres } from './postgres.ts'

const [action, ...extra] = process.argv.slice(2)
const postgres = new LocalPostgres({ ...process.env })
try {
  if (extra.length || !['start', 'url', 'stop', 'clean'].includes(action))
    throw new Error('Usage: node scripts/postgres-cli.mjs start|url|stop|clean')
  switch (action) {
    case 'start':
      console.log(`Postgres is ready at ${await postgres.start()}`)
      break
    case 'url':
      console.log(await postgres.url())
      break
    case 'stop':
      await postgres.stop()
      break
    case 'clean':
      await postgres.clean()
      break
  }
} catch (error) {
  console.error(error.message)
  process.exitCode = 1
}
