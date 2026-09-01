export interface CreateWorkspaceActionData {
  error: string
  name: string
}

const WORKSPACE_NAME_CHARACTERS = /^[a-z0-9-]+$/

export function validateWorkspaceName(name: string): string | null {
  if (name.trim().length === 0) return 'Workspace name is required'
  if (name.length > 63) return 'Workspace name must be 63 characters or fewer'
  if (name.startsWith('-') || name.endsWith('-')) {
    return 'Workspace name must not start or end with a hyphen'
  }
  if (!WORKSPACE_NAME_CHARACTERS.test(name)) {
    return 'Workspace name may only contain letters, numbers, and hyphens'
  }
  return null
}
