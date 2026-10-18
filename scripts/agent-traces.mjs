#!/usr/bin/env node
// Exports Coral agent threads as JSONL training traces: one line per Task.
//
//   node scripts/agent-traces.mjs [--db .dev/state/coral.db] [--task <id>] > traces.jsonl
//
// Each line holds the Task, its turns (caller message, final message, evidence,
// timing, progress events) and the model transcript exactly as the model saw it.
// The system prompt and tool list are not stored; they come from the build that ran.
import { DatabaseSync } from 'node:sqlite'
import { parseArgs } from 'node:util'

const { values } = parseArgs({
  options: {
    db: { type: 'string', default: '.dev/state/coral.db' },
    task: { type: 'string' },
  },
})

const db = new DatabaseSync(values.db, { readOnly: true })
// Timestamps are Unix nanoseconds, beyond a JavaScript number.
const prepare = (sql) => {
  const statement = db.prepare(sql)
  statement.setReadBigInts(true)
  return statement
}
const nanos = (value) => (value == null ? null : new Date(Number(BigInt(value) / 1_000_000n)).toISOString())
const durationMs = (start, end) => (end == null ? null : Number((BigInt(end) - BigInt(start)) / 1_000_000n))

const tasks = values.task
  ? prepare('SELECT * FROM task_records WHERE id = ?').all(values.task)
  : prepare(
        `SELECT t.* FROM task_records t
         WHERE EXISTS (SELECT 1 FROM agent_turns a WHERE a.task_id = t.id)
         ORDER BY t.started_at_unix_nanos`,
      )
      .all()

const turnsOf = prepare('SELECT * FROM agent_turns WHERE task_id = ? ORDER BY sequence')
const eventsOf = prepare('SELECT * FROM agent_turn_events WHERE turn_id = ? ORDER BY sequence')
const messagesOf = prepare('SELECT * FROM agent_thread_messages WHERE task_id = ? ORDER BY sequence')

for (const task of tasks) {
  const messages = messagesOf.all(task.id).map((row) => ({
    turn_id: row.turn_id,
    role: row.role,
    content: JSON.parse(row.content_json),
    bytes: row.content_json.length,
  }))
  const turns = turnsOf.all(task.id).map((turn) => {
    const own = messages.filter((message) => message.turn_id === turn.id)
    const toolCalls = own
      .filter((message) => message.role === 'assistant')
      .flatMap((message) => message.content.filter((block) => block.type === 'tool_use'))
    return {
      turn_id: turn.id,
      sequence: Number(turn.sequence),
      state: turn.state,
      message: turn.message,
      final: turn.final_message,
      evidence: turn.evidence_json ? JSON.parse(turn.evidence_json) : null,
      failure: turn.failure,
      started_at: nanos(turn.started_at_unix_nanos),
      duration_ms: durationMs(turn.started_at_unix_nanos, turn.ended_at_unix_nanos),
      model_calls: own.filter((message) => message.role === 'assistant').length,
      tool_calls: toolCalls.map((block) => block.name),
      transcript_bytes: own.reduce((sum, message) => sum + message.bytes, 0),
      events: eventsOf.all(turn.id).map((event) => ({
        sequence: Number(event.sequence),
        at: nanos(event.occurred_at_unix_nanos),
        ...JSON.parse(event.payload_json),
      })),
    }
  })
  process.stdout.write(
    JSON.stringify({
      task_id: task.id,
      intent: task.intent,
      outcome: task.outcome,
      turns,
      messages: messages.map(({ turn_id, role, content }) => ({ turn_id, role, content })),
    }) + '\n',
  )
}
