/**
 * The Slack adapter.
 *
 * `fetch` refreshes the pinned snapshot from Slack's documentation and the
 * recorded samples in `slackapi/java-slack-sdk`. `extract` and `build` arrive
 * with the later stages of the pipeline.
 */

import process from 'node:process'

import type { Adapter } from '../registry.ts'
import type { ApiModel } from '../../core/model.ts'
import { fetchAllBytes, fetchText } from '../../core/http.ts'
import { loadConfig } from '../../core/config.ts'
import { pruneSample } from '../../core/prune.ts'
import { SnapshotWriter } from '../../core/snapshot.ts'
import {
  joinIndexes,
  parseMethodIndex,
  parseSampleIndex,
  SAMPLE_INDEX_URL,
  selectScope,
  SITEMAP_URL,
  type DiscoveredMethod,
} from './discover.ts'

export const SLACK_ADAPTER: Adapter = {
  name: 'slack',
  fetch: fetchSnapshot,
  // The extract and emit stages land with the later layers of this stack; the
  // snapshot they read has to exist first.
  extract(): Promise<ApiModel> {
    throw new Error("slack adapter: 'extract' is not implemented yet")
  },
  build(): Promise<boolean> {
    throw new Error("slack adapter: 'build' is not implemented yet")
  },
}

export function docsPath(method: DiscoveredMethod): string {
  return `docs/${method.slug}.md`
}

export function samplePath(method: DiscoveredMethod): string {
  return `samples/${method.name}.json`
}

function log(message: string): void {
  process.stderr.write(`${message}\n`)
}

async function fetchSnapshot(): Promise<void> {
  const config = await loadConfig('slack')

  log('fetching indexes')
  const [sitemap, sampleIndex] = await Promise.all([
    fetchText(SITEMAP_URL),
    fetchText(SAMPLE_INDEX_URL),
  ])
  const { methods, samplesWithoutDocs } = joinIndexes(
    parseMethodIndex(sitemap),
    parseSampleIndex(sampleIndex),
  )
  log(`  ${methods.length} documented methods, ${samplesWithoutDocs.length} samples without docs`)

  const { selected, missing } = selectScope(methods, config.methods)
  if (missing.length > 0) {
    throw new Error(
      `configured methods are no longer documented by Slack: ${missing.join(', ')}. ` +
        `Remove them from apis/slack/config.yaml, or check whether they were renamed.`,
    )
  }
  const undocumented = methods.length - config.methods.length
  log(`  ${selected.length} in scope, ${undocumented} documented methods out of scope`)

  const urls = selected.flatMap((method) =>
    method.sampleUrl === undefined ? [method.docsUrl] : [method.docsUrl, method.sampleUrl],
  )
  log(`downloading ${urls.length} files`)
  const bodies = await fetchAllBytes(urls)

  const writer = new SnapshotWriter(config.snapshotDir)
  let sampleBytes = 0
  let prunedBytes = 0
  for (const method of selected) {
    const docs = bodies.get(method.docsUrl)
    if (docs === undefined) {
      throw new Error(`no response for ${method.docsUrl}`)
    }
    writer.add(docsPath(method), method.docsUrl, docs, new TextDecoder().decode(docs))

    if (method.sampleUrl === undefined) {
      log(`  ${method.name}: no recorded response sample`)
      continue
    }
    const sample = bodies.get(method.sampleUrl)
    if (sample === undefined) {
      throw new Error(`no response for ${method.sampleUrl}`)
    }
    // Samples are stored pruned: the raw ones run to megabytes of Block Kit
    // nesting that no Coral column ever reads. The manifest still pins the
    // digest of the raw bytes, so upstream changes remain detectable.
    const pruned = `${JSON.stringify(
      pruneSample(JSON.parse(new TextDecoder().decode(sample))),
      undefined,
      2,
    )}\n`
    sampleBytes += sample.length
    prunedBytes += pruned.length
    writer.add(samplePath(method), method.sampleUrl, sample, pruned)
  }

  const manifest = await writer.commit(new Date().toISOString())
  log(
    `wrote ${manifest.inputs.length} inputs to apis/slack/snapshot ` +
      `(samples pruned ${formatBytes(sampleBytes)} to ${formatBytes(prunedBytes)})`,
  )
}

function formatBytes(bytes: number): string {
  return bytes < 1024 * 1024
    ? `${Math.round(bytes / 1024)} KB`
    : `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}
