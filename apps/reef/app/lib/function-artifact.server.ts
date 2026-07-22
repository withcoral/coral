import { parse, stringify } from 'yaml'

export interface FunctionArtifact {
  description: string
  name: string
  schema: string
  sql: string
}

const fields = new Set(['description', 'name', 'schema'])

export function parseFunctionArtifact(raw: string): FunctionArtifact {
  if (!raw.startsWith('/*')) throw new Error('Function artifact must start with frontmatter')

  const commentEnd = raw.indexOf('*/', 2)
  if (commentEnd === -1) throw new Error("Function frontmatter must end with '*/'")

  const frontmatter = parse(raw.slice(2, commentEnd))
  if (!isRecord(frontmatter)) throw new Error('Function frontmatter must be a mapping')
  const unknownField = Object.keys(frontmatter).find((field) => !fields.has(field))
  if (unknownField) throw new Error(`Function frontmatter has an unknown field: ${unknownField}`)

  return {
    description: stringField(frontmatter, 'description', false),
    name: stringField(frontmatter, 'name', true),
    schema: stringField(frontmatter, 'schema', true),
    sql: raw.slice(commentEnd + 2).trim(),
  }
}

export function formatFunctionArtifact(artifact: FunctionArtifact): string {
  if (artifact.description.includes('*/')) {
    throw new Error("description cannot contain '*/'")
  }
  const frontmatter = stringify({
    name: artifact.name.trim(),
    schema: artifact.schema.trim(),
    description: artifact.description,
  }).trim()
  return `/*\n${frontmatter}\n*/\n\n${artifact.sql.trim()}`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function stringField(
  frontmatter: Record<string, unknown>,
  field: 'description' | 'name' | 'schema',
  required: boolean,
): string {
  const value = frontmatter[field]
  if (value === undefined && !required) return ''
  if (typeof value !== 'string') throw new Error(`Function ${field} must be a string`)
  return value
}
