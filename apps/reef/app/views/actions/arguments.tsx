import { Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Disclosure } from '@/wax/components/disclosure'
import * as DescriptionList from '@/wax/components/description-list'

/** Exact logical arguments, shared with approval review. */
export function Arguments({ source }: { source: string }) {
  const fields = argumentFields(source)
  return (
    <Disclosure title={fields ? `Arguments (${fields.length})` : 'Arguments'} open>
      {fields?.length === 0 ? (
        <Typography.BodySmall>This call has no arguments.</Typography.BodySmall>
      ) : fields ? (
        <DescriptionList.List>
          {fields.map(([key, value]) => (
            <DescriptionList.Row key={key} label={key}>
              {/* JSON text keeps "2" apart from 2 and "" visible. */}
              {value !== null && typeof value === 'object' ? (
                <CodeBlock value={JSON.stringify(value)} format="json" />
              ) : (
                <Typography.Code>{JSON.stringify(value)}</Typography.Code>
              )}
            </DescriptionList.Row>
          ))}
        </DescriptionList.List>
      ) : (
        <CodeBlock value={source} format="json" />
      )}
    </Disclosure>
  )
}

/** One row per field when the arguments are a JSON object that each row shows exactly.
 * Otherwise undefined: an imprecise number or a duplicate key would change on parsing,
 * so the reviewer reads the original JSON instead. */
function argumentFields(source: string): [string, unknown][] | undefined {
  let value: unknown
  try {
    value = JSON.parse(source)
  } catch {
    return undefined
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined
  // Compare without whitespace outside strings.
  const compact = source.replace(/"(?:\\.|[^"\\])*"|\s+/g, (token) =>
    token.startsWith('"') ? token : '',
  )
  if (JSON.stringify(value) !== compact) return undefined
  return Object.entries(value)
}
