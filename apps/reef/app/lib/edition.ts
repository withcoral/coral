import { createContext, useContext } from 'react'

export type FeatureState = 'enabled' | 'needs_enterprise'

export interface EnterpriseFeatureView {
  /** Stable id, in lowercase words joined by hyphens. */
  id: string
  displayName: string
  state: FeatureState
}

/** The deployment's Edition, and each Enterprise Feature this build contains. */
export interface EditionView {
  edition: 'community' | 'enterprise'
  features: EnterpriseFeatureView[]
}

/**
 * The Edition the app shell read for this request, or null when Coral could
 * not answer. Pages read it here, not from Coral. Its use still decides.
 */
export const EditionContext = createContext<EditionView | null>(null)

export function useEdition() {
  return useContext(EditionContext)
}

/**
 * The feature `id` names, or undefined when this build lacks it or Coral
 * could not answer.
 */
export function useEnterpriseFeature(id: string) {
  return useEdition()?.features.find((feature) => feature.id === id)
}
