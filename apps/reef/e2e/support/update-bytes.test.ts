import assert from 'node:assert/strict'
import { test } from 'node:test'

import { updateBytes } from './update-bytes.ts'

// A final update that lean-updates.test.ts received, with its Run identity and
// final value as placeholders. Both appear in the text copy and the structured one.
const received = String.raw`{"content":[{"type":"text","text":"{\"run_id\":\"$RUN\",\"state\":\"succeeded\",\"required_actions\":[],\"events\":[],\"next_cursor\":\"BI5q2pfcAmCa0gaslm01oncAAAAAAAABAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"has_more\":false,\"final_value\":{\"kind\":\"value\",\"value\":$VALUE}}"}],"structuredContent":{"run_id":"$RUN","state":"succeeded","required_actions":[],"events":[],"next_cursor":"BI5q2pfcAmCa0gaslm01oncAAAAAAAABAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","has_more":false,"final_value":{"kind":"value","value":$VALUE}},"isError":false}`
const observed = (run: string, value = '128') => ({
  responses: [received.replaceAll('$RUN', run).replaceAll('$VALUE', value)],
  update: { run_id: run },
})
// A short and a long valid four-word Run identity.
const shortest = 'air-art-big-box'
const longest = 'application-description-development-information'

test('update bytes leave out the Run identity, whatever its length', () => {
  const uuid = '0b3c5c7e-8f61-4a52-9d2e-6a1f0c9b7d34'
  const sizes = [shortest, longest, uuid].map((run) => updateBytes(observed(run)))
  assert.deepEqual(sizes, [sizes[0], sizes[0], sizes[0]])
  assert.equal(sizes[0], Buffer.byteLength(observed('').responses[0]))
})

test('update bytes count every byte the update gains', () => {
  for (const [from, to] of [
    [shortest, longest],
    [longest, shortest],
  ]) {
    // Two more digits in each copy.
    assert.equal(updateBytes(observed(to, '12800')) - updateBytes(observed(from)), 4)
    // Eight more digits in each copy reach lean-updates' 16-byte bound.
    assert.equal(updateBytes(observed(to, '12800000000')) - updateBytes(observed(from)), 16)
  }
})
