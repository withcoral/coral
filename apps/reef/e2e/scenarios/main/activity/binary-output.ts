import { expect, type TestContext } from 'vitest'

import { createWorkspace, programRunner } from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'
import { retainedPayload, shownPayload } from '../../../support/payloads.ts'

// Bytes that are not UTF-8 text, with a CR LF that text handling would change.
const image = {
  _tag: 'BinaryValue',
  encoding: 'base64',
  mimeType: 'image/png',
  data: 'AP+ADQo=',
  byteLength: 5,
}

// Too long for Reef to mount whole: only its rows near the view show.
const items = 10_000
const huge = Array.from({ length: items }, (_, index) => `item-${String(index).padStart(5, '0')}`)

/**
 * A Program's binary Final Value crosses every process unchanged: V8 to Coral's
 * store, then to an MCP client as an image block and through private gRPC to
 * Reef's Run Status Page. Value rules stay in `crates/coral-run/tests/domain/binary.rs`.
 * A Final Value too long to show whole still reaches Reef whole.
 */
export async function binaryOutput(context: TestContext) {
  await withOwner(context, async ({ page, target, step, accessToken }) => {
    const workspace = await createWorkspace(accessToken, `Binary output ${crypto.randomUUID()}`)
    let runName = ''
    await step('An MCP client receives the returned bytes as an image block', async () => {
      const result = await programRunner(page, target, workspace).exec(
        `return ${JSON.stringify(image)};`,
      )
      expect(result.isError).not.toBe(true)
      const update = result.structuredContent
      expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
      expect(update.final_value).toEqual({ kind: 'content', content_index: 1 })
      expect(result.content?.[1]).toEqual({
        type: 'image',
        data: image.data,
        mimeType: image.mimeType,
      })
      runName = `workspaces/${workspace}/runs/${update.run_id}`
    })
    await step('Reef shows the same value on the Run Status Page', async () => {
      await page.goto(`/${runName}`)
      const value = page.getByRole('region', { name: 'Final value', exact: true })
      await shownPayload(value, image.data).waitFor()
      expect(JSON.parse(await retainedPayload(page, value, 'Final value'))).toEqual(image)
    })
    await step('Reef keeps a Final Value too long to show whole, exactly', async () => {
      const result = await programRunner(page, target, workspace).exec(
        `return Array.from({ length: ${items} }, (_, index) => \`item-\${String(index).padStart(5, '0')}\`);`,
      )
      const update = result.structuredContent
      expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
      await page.goto(`/workspaces/${workspace}/runs/${update.run_id}`)
      const value = page.getByRole('region', { name: 'Final value', exact: true })
      await shownPayload(value, huge[0]!).waitFor()
      expect(JSON.parse(await retainedPayload(page, value, 'Final value'))).toEqual(huge)
    })
  })
}
