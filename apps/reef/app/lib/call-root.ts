/**
 * Provider Call Roots. Coral reports each provider's complete Call Root; Reef
 * sends only the short namespace of a provider an owner adds.
 */

/** The Built-in Provider's Call Root, which holds every other root. */
const BUILTIN_ROOT = 'coral'
/** The root of every Private Tool's Call Path. */
const PRIVATE_ROOT = 'coral.private.'
/** The members of `coral` that name no Built-in Tool. */
const NON_TOOL_MEMBERS = new Set(['providers', 'private', 'shared', 'emit', 'ToolError'])
/** Each old root that a retained Run may have recorded, and the root that replaced it. */
const OLD_ROOTS = [
  ['coral.workspace.', 'coral.'],
  ['coral.functions.', PRIVATE_ROOT],
] as const

/** `callPath` under its current root. A retained Run keeps the old roots it recorded. */
function currentCallPath(callPath: string) {
  for (const [old, current] of OLD_ROOTS)
    if (callPath.startsWith(old)) return current + callPath.slice(old.length)
  return callPath
}

/** The complete Call Path of one tool of a provider. */
export function toolCallPath(callRoot: string, relativePath: string) {
  return `${callRoot}.${relativePath}`
}

/**
 * The provider whose Call Root owns this Call Path, and the tool path inside
 * it. The Built-in Call Root `coral` holds the others, but it owns only one
 * segment that no other member of `coral` takes, so at most one provider
 * owns a path.
 */
export function toolOwner<P extends { callRoot: string }>(
  callPath: string,
  providers: readonly P[],
) {
  const path = currentCallPath(callPath)
  for (const provider of providers) {
    const prefix = `${provider.callRoot}.`
    if (!path.startsWith(prefix)) continue
    const relativePath = path.slice(prefix.length)
    const owned =
      provider.callRoot === BUILTIN_ROOT
        ? relativePath.length > 0 &&
          !relativePath.includes('.') &&
          !NON_TOOL_MEMBERS.has(relativePath)
        : relativePath.length > 0
    if (owned) return { provider, relativePath }
  }
  return null
}

/** The path under `coral.private` of a Private Tool's complete Call Path, or null. */
export function privateToolPath(callPath: string) {
  const path = currentCallPath(callPath)
  return path.startsWith(PRIVATE_ROOT) && path.length > PRIVATE_ROOT.length
    ? path.slice(PRIVATE_ROOT.length)
    : null
}
