import {
  Edition,
  EnterpriseFeatureState,
  type GetEditionResponse,
} from '@/generated/coral/management/v1/edition_pb'
import type { EditionView, EnterpriseFeatureView } from './edition'

/**
 * What Reef shows of Coral's answer. A state newer than this Reef reads as
 * needing Enterprise, and an Edition newer than it as Community, so nothing
 * shows as working that may not.
 */
export function editionView(response: GetEditionResponse): EditionView {
  return {
    edition: response.edition === Edition.ENTERPRISE ? 'enterprise' : 'community',
    features: response.features.map((feature): EnterpriseFeatureView => ({
      id: feature.id,
      displayName: feature.displayName,
      state: feature.state === EnterpriseFeatureState.ENABLED ? 'enabled' : 'needs_enterprise',
    })),
  }
}
