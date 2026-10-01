interface McpClientMatchable {
  readonly id: string
  readonly name: string
}

// Ids are matched alongside names so a reader who types how the client is
// spelled on disk ("vscode", "copilot") finds the row titled "VS Code".
export function filterMcpClients<T extends McpClientMatchable>(
  clients: readonly T[],
  search: string,
): readonly T[] {
  const query = search.trim().toLowerCase()
  if (!query) return clients

  return clients.filter(
    (client) =>
      client.name.toLowerCase().includes(query) || client.id.toLowerCase().includes(query),
  )
}
