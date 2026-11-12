import { SecretStoreState } from '@/generated/coral/management/v1/secret_store_pb'

/**
 * Why Coral cannot read from a secret store right now, as Wax `Status`
 * blockers. Empty when it can. Reads still decide; this only warns early.
 */
export function secretStoreBlockers(state: SecretStoreState): string[] {
  switch (state) {
    case SecretStoreState.AVAILABLE:
      return []
    case SecretStoreState.NEEDS_ENTERPRISE:
      return ['Needs Enterprise']
    case SecretStoreState.KIND_MISSING:
      return ['Not in this build']
    case SecretStoreState.REMOVED:
      return ['Removed from config']
    default:
      return ['Unknown state']
  }
}
