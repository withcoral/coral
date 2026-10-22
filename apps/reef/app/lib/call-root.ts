/**
 * Provider Call Roots. Coral reports each provider's complete Call Root; Reef
 * sends only the short namespace of a provider an owner adds.
 */

/** The complete Call Path of one operation of a provider. */
export function operationCallPath(callRoot: string, relativePath: string) {
  return `${callRoot}.${relativePath}`
}

/**
 * The provider whose Call Root owns this Call Path, and the operation path
 * inside it. Call Roots never nest, so at most one provider owns a path.
 */
export function operationOwner<P extends { callRoot: string }>(
  callPath: string,
  providers: readonly P[],
) {
  for (const provider of providers) {
    const prefix = `${provider.callRoot}.`
    if (callPath.startsWith(prefix) && callPath.length > prefix.length)
      return { provider, relativePath: callPath.slice(prefix.length) }
  }
  return null
}
