export function errorMessage(error: unknown): string {
  if (error instanceof Response) throw error
  return error instanceof Error ? error.message : String(error)
}

export function trimTrailingSlash(value: string): string {
  return value.endsWith('/') ? value.slice(0, -1) : value
}
