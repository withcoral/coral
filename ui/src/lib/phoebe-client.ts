const DEFAULT_SERVER_URL = 'http://localhost:1457'

export function formatApiError(error: string, serverUrl?: string): string {
  if (error.startsWith('401') || error.includes('Authentication required')) {
    return `Authentication required. Log in first: coral user login --server ${serverUrl ?? DEFAULT_SERVER_URL}`
  }
  return error
}
