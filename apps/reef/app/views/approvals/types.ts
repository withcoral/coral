import type { RecipeIcons } from '@/lib/provider-recipes/choice'

/** A provider of the workspace, as the Approvals page names the calls under its Call Root. */
export interface ProviderMark {
  callRoot: string
  displayName: string
  icons?: RecipeIcons
}
