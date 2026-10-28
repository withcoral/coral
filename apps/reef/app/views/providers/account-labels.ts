import { useLocalDeployment } from '@/lib/deployment'
import type { AccountChoice } from '@/lib/provider-publication-attempt/model'

/** Shared presentation copy; no transport or authority decisions belong here. */
export const accountLabels = {
  none: 'No account',
  fixed: 'One shared account',
  caller: 'Individual accounts',
} as const satisfies Record<AccountChoice, string>

export type AccountLabel = (typeof accountLabels)[AccountChoice]

/**
 * Who else uses a provider's account. A workspace has other people; a local
 * deployment has only you and your agents.
 */
const audiences = {
  workspace: {
    shareTitle: 'Share this account with the workspace?',
    shareBody: 'You share this account with everyone in this workspace.',
    free: 'Coral sends no credentials when a program calls this provider, so everyone in this workspace can use it as soon as you add it.',
    sharedCallers: 'Everyone in this workspace calls the provider with',
    others: 'Other people connect theirs when they first use the provider.',
    later: 'Everyone, including you, connects an account when they first use the provider.',
    onlyYou: 'Only you use this account. Other people connect their own accounts.',
    sharedAccount: 'People and agents use this shared account.',
    individualAccounts:
      'Each person connects their own account. Workspace owners connect accounts for agents.',
    reconnect:
      'Reconnect your account after you save the changes. Reconnect each agent account. Other users reconnect their own accounts.',
    help: {
      fixed: 'People and agents use one shared account for calls through this namespace.',
      caller:
        'Connect your own account when you use this provider. Workspace owners connect accounts for agents.',
    },
  },
  local: {
    shareTitle: 'Share this account with your agents?',
    shareBody: 'You and your agents use this account.',
    free: 'Coral sends no credentials when a program calls this provider, so you and your agents can use it as soon as you add it.',
    sharedCallers: 'You and your agents call the provider with',
    others: 'You connect a separate account for each agent that uses it.',
    later:
      'You connect your account when you first use the provider. You connect one for each agent that uses it.',
    onlyYou: 'Only you use this account. You connect a separate account for each agent.',
    sharedAccount: 'You and your agents use this shared account.',
    individualAccounts: 'You connect your own account and a separate account for each agent.',
    reconnect: 'Reconnect your account after you save the changes. Reconnect each agent account.',
    help: {
      fixed: 'You and your agents use one shared account for calls through this namespace.',
      caller:
        'Connect your own account when you use this provider. Connect a separate account for each agent.',
    },
  },
}

export type AccountAudience = (typeof audiences)['workspace']

export function useAccountAudience(): AccountAudience {
  return audiences[useLocalDeployment() ? 'local' : 'workspace']
}

/** What each account choice means for the people and agents who call the provider. */
export function accountHelp(audience: AccountAudience): Record<AccountChoice, string> {
  return { none: 'Coral sends no account credentials with tool calls.', ...audience.help }
}
