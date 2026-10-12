import { Button } from '@/wax/components'

/** The primary action of an empty state that fills once an MCP client connects. */
export function CopyMcpEndpoint({ endpoint }: { endpoint?: string }) {
  if (!endpoint) return null
  return (
    <Button.CopyButton ariaLabel="Copy MCP URL" textToCopy={endpoint} variant="primary">
      <Button.Text>Copy MCP URL</Button.Text>
    </Button.CopyButton>
  )
}
