interface WorkspaceUserMatchable {
  readonly displayName?: string
  readonly userId: string
}

// Match stable ids alongside display names so users remain findable when a
// display name is missing, duplicated, or different from the value an owner knows.
export function filterWorkspaceUsers<T extends WorkspaceUserMatchable>(
  users: readonly T[],
  search: string,
): readonly T[] {
  const query = search.trim().toLowerCase()
  if (!query) return users

  return users.filter(
    (user) =>
      user.displayName?.toLowerCase().includes(query) || user.userId.toLowerCase().includes(query),
  )
}
