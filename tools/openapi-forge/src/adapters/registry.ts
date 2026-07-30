/**
 * Adapter registry.
 *
 * An adapter owns everything provider-specific: where the upstream inputs live,
 * how to read them, and how to turn them into an {@link ApiModel}. Registering
 * one here is the only wiring a new API needs.
 */

import type { ApiModel } from '../core/model.ts'
import { SLACK_ADAPTER } from './slack/index.ts'

export interface BuildOptions {
  /** Report drift instead of writing the descriptor. */
  check: boolean
}

export interface Adapter {
  /** API identifier; matches the directory under `apis/`. */
  name: string
  /** Refresh the pinned snapshot from upstream. The only networked stage. */
  fetch(): Promise<void>
  /** Regenerate the descriptor from the snapshot. Returns false on failure. */
  build(options: BuildOptions): Promise<boolean>
  /** Read the snapshot into the vendor-neutral model. Exposed for tests. */
  extract(): Promise<ApiModel>
}

const ADAPTERS: Adapter[] = [SLACK_ADAPTER]

export function findAdapter(name: string): Adapter | undefined {
  return ADAPTERS.find((adapter) => adapter.name === name)
}

export function adapterNames(): string[] {
  return ADAPTERS.map((adapter) => adapter.name)
}
