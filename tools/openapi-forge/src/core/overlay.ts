/**
 * Hand-written corrections, applied to the model before emission.
 *
 * Extraction is only as good as what providers publish, and providers get
 * things wrong. The overlay is where a human says so — and it is deliberately
 * the *only* hand-edited input, so regenerating never discards their work.
 *
 * Every override must apply to something. An override that matches nothing is
 * an error rather than a no-op: overlays exist because upstream was wrong, and
 * the moment upstream is fixed or renamed, a silently-dead correction is
 * indistinguishable from one that is still doing its job.
 */

import { readFile } from 'node:fs/promises'

import { parse } from 'yaml'

import type { ApiModel, Operation, Parameter, ScalarType } from './model.ts'

export class OverlayError extends Error {}

export interface ParameterOverride {
  /** Correct a mis-documented scalar type. */
  type?: ScalarType
  description?: string
  required?: boolean
  /** Replace the documented default; `null` removes it. */
  default?: string | number | boolean | null
  /** Remove the parameter entirely. */
  drop?: boolean
  /** Why this correction exists. Documentation for the next reader. */
  reason?: string
}

export interface ResponseOverride {
  /**
   * Top-level response properties to leave undescribed.
   *
   * An OpenAPI schema is a description, not an inventory, and Coral reads only
   * what is described. The lever exists for envelopes that pair a resource
   * with an incidental array: row-path inference sees the array, makes it the
   * rows, and discards the resource the operation is named after. Omitting the
   * array is what keeps the operation returning what it says it returns.
   */
  dropProperties?: string[]
  reason?: string
}

export interface OperationOverride {
  summary?: string
  description?: string
  deprecated?: boolean
  /** Remove the operation entirely. */
  drop?: boolean
  parameters?: Record<string, ParameterOverride>
  response?: ResponseOverride
  reason?: string
}

export interface Overlay {
  /** Applied to this parameter wherever it appears. */
  parameters?: Record<string, ParameterOverride>
  /** Applied to one operation, keyed by its provider identifier. */
  operations?: Record<string, OperationOverride>
}

export async function loadOverlay(path: string): Promise<Overlay> {
  let raw: string
  try {
    raw = await readFile(path, 'utf8')
  } catch {
    // An API with nothing to correct needs no overlay.
    return {}
  }
  const parsed = parse(raw) as Overlay | null
  if (parsed === null) {
    return {}
  }
  if (typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new OverlayError(`${path} is not a YAML mapping`)
  }
  return parsed
}

/**
 * Apply `overlay` to `model`.
 *
 * @throws OverlayError when an override matches nothing.
 */
export function applyOverlay(model: ApiModel, overlay: Overlay): ApiModel {
  const usedGlobalParameters = new Set<string>()
  const usedOperations = new Set<string>()
  const usedOperationParameters = new Set<string>()
  const usedDroppedProperties = new Set<string>()

  const operations: Operation[] = []
  for (const operation of model.operations) {
    const override = overlay.operations?.[operation.id]
    if (override !== undefined) {
      usedOperations.add(operation.id)
    }
    if (override?.drop === true) {
      continue
    }
    operations.push(
      applyToOperation(operation, overlay, override, {
        usedGlobalParameters,
        usedOperationParameters,
        usedDroppedProperties,
      }),
    )
  }

  const unused = [
    ...Object.keys(overlay.parameters ?? {})
      .filter((name) => !usedGlobalParameters.has(name))
      .map((name) => `parameters.${name}`),
    ...Object.keys(overlay.operations ?? {})
      .filter((id) => !usedOperations.has(id))
      .map((id) => `operations.${id}`),
    ...Object.entries(overlay.operations ?? {}).flatMap(([id, override]) =>
      Object.keys(override.parameters ?? {})
        .filter((name) => !usedOperationParameters.has(`${id}.${name}`))
        .map((name) => `operations.${id}.parameters.${name}`),
    ),
    ...Object.entries(overlay.operations ?? {}).flatMap(([id, override]) =>
      (override.response?.dropProperties ?? [])
        .filter((name) => !usedDroppedProperties.has(`${id}.${name}`))
        .map((name) => `operations.${id}.response.dropProperties.${name}`),
    ),
  ].toSorted((left, right) => left.localeCompare(right))

  if (unused.length > 0) {
    throw new OverlayError(
      `overlay entries match nothing: ${unused.join(', ')}. ` +
        `Upstream may have fixed or renamed them; remove the entries or update them.`,
    )
  }

  return { ...model, operations }
}

interface Usage {
  usedGlobalParameters: Set<string>
  usedOperationParameters: Set<string>
  usedDroppedProperties: Set<string>
}

function applyToOperation(
  operation: Operation,
  overlay: Overlay,
  override: OperationOverride | undefined,
  usage: Usage,
): Operation {
  const parameters: Parameter[] = []
  for (const parameter of operation.parameters) {
    const global = overlay.parameters?.[parameter.name]
    if (global !== undefined) {
      usage.usedGlobalParameters.add(parameter.name)
    }
    const specific = override?.parameters?.[parameter.name]
    if (specific !== undefined) {
      usage.usedOperationParameters.add(`${operation.id}.${parameter.name}`)
    }
    // A per-operation override is the more specific statement, so it wins.
    const merged = { ...global, ...specific }
    if (Object.keys(merged).length === 0) {
      parameters.push(parameter)
      continue
    }
    if (merged.drop === true) {
      continue
    }
    parameters.push(applyToParameter(parameter, merged))
  }

  return {
    ...operation,
    ...(override?.summary === undefined ? {} : { summary: override.summary }),
    ...(override?.description === undefined ? {} : { description: override.description }),
    ...(override?.deprecated === undefined ? {} : { deprecated: override.deprecated }),
    parameters,
    response: dropResponseProperties(operation, override?.response?.dropProperties, usage),
  }
}

function dropResponseProperties(
  operation: Operation,
  names: string[] | undefined,
  usage: Usage,
): Operation['response'] {
  if (names === undefined || names.length === 0 || operation.response.kind !== 'object') {
    return operation.response
  }
  const properties = { ...operation.response.properties }
  for (const name of names) {
    if (properties[name] === undefined) {
      continue
    }
    usage.usedDroppedProperties.add(`${operation.id}.${name}`)
    delete properties[name]
  }
  return { ...operation.response, properties }
}

function applyToParameter(parameter: Parameter, override: ParameterOverride): Parameter {
  const updated: Parameter = {
    ...parameter,
    ...(override.description === undefined ? {} : { description: override.description }),
    ...(override.required === undefined ? {} : { required: override.required }),
    ...(override.type === undefined
      ? {}
      : { schema: { ...parameter.schema, type: override.type } }),
  }
  if (override.default === null) {
    delete updated.default
  } else if (override.default !== undefined) {
    updated.default = override.default
  }
  return updated
}
