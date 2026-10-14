import * as Card from '@/wax/components/card'
import { CodeBlock } from '@/wax/components/code-block'

/** One exact retained value in a card: a region named for assistive technology, with no
 * visible title. JSON keeps its exact tokens; text keeps its line breaks. */
export function ValueCard({
  value,
  format,
  label,
}: {
  value: string
  format: 'text' | 'json'
  label: string
}) {
  return (
    <Card.Container as="section" aria-label={label}>
      <CodeBlock value={value} format={format} />
    </Card.Container>
  )
}
