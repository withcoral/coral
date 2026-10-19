import assert from 'node:assert/strict'
import test from 'node:test'
import { Toxiproxy, parseArgs, plan } from './load.mjs'

/** Answers every toxiproxy API call with the status configured for its method, recording the calls and their bodies. */
function stubToxiproxyApi(t, statusByMethod) {
  const calls = []
  const bodies = []
  t.mock.method(globalThis, 'fetch', async (url, init = {}) => {
    const method = init.method ?? 'GET'
    calls.push(`${method} ${new URL(url).pathname}`)
    if (init.body !== undefined) bodies.push(JSON.parse(init.body))
    return new Response(null, { status: statusByMethod[method] ?? 200 })
  })
  return { calls, bodies }
}

test('a latency step fails when the previous toxic cannot be removed, before a new one is added', async (t) => {
  const { calls, bodies } = stubToxiproxyApi(t, { DELETE: 500 })
  await assert.rejects(new Toxiproxy({}).setLatency(5), /latency toxic removal failed: 500/)
  assert.deepEqual(calls, ['DELETE /proxies/coral-load-pg/toxics/latency'])
  assert.deepEqual(bodies, [])
})

test('a missing toxic counts as removed and the requested latency is added', async (t) => {
  const { calls, bodies } = stubToxiproxyApi(t, { DELETE: 404, POST: 200 })
  await new Toxiproxy({}).setLatency(5)
  assert.deepEqual(calls, ['DELETE /proxies/coral-load-pg/toxics/latency', 'POST /proxies/coral-load-pg/toxics'])
  assert.deepEqual(bodies, [
    { name: 'latency', type: 'latency', stream: 'downstream', toxicity: 1, attributes: { latency: 5, jitter: 0 } },
  ])
})

test('no added latency only removes the toxic', async (t) => {
  const { calls, bodies } = stubToxiproxyApi(t, { DELETE: 204 })
  await new Toxiproxy({}).setLatency(0)
  assert.deepEqual(calls, ['DELETE /proxies/coral-load-pg/toxics/latency'])
  assert.deepEqual(bodies, [])
})

test('options that would run a different workload than requested are rejected', async () => {
  for (const [argv, message] of [
    [['--calls', '0'], /--calls needs an integer of at least 1/],
    [['--calls', '30,'], /--calls needs an integer of at least 1/],
    [['--emits', 'abc'], /--emits needs an integer/],
    [['--latency', ''], /--latency needs a value/],
    [['--latency', '-5'], /--latency needs an integer of at least 0/],
    [['--runs'], /--runs needs a value/],
    [['--cells', 'nope'], /Unknown cell nope/],
    [['--mode', 'sometimes'], /--mode is approve or hold/],
  ]) {
    await assert.rejects(parseArgs(argv), message, argv.join(' '))
  }
})

test('the defaults run every cell once per latency step', async () => {
  const options = await parseArgs([])
  assert.deepEqual(plan(options, 0).map((cell) => [cell.test, cell.stem]), [
    ['run_fan_out_load', 'postgres-fanout-30-0ms'],
    ['run_emit_load', 'postgres-emit-small-0ms'],
    ['run_emit_load', 'postgres-emit-64k-0ms'],
    ['run_concurrent_runs_load', 'postgres-concurrent-20-0ms'],
  ])
})
