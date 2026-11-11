// Fixtures the provider stories and the Tools page stories share.

import type { SourceChoice } from '@/lib/provider-recipes/choice'
import githubIcon from '../../../recipes/github/github.svg'
import stripeIcon from '../../../recipes/stripe/stripe.svg'

export const form = { csrf: 'storybook-csrf', requestId: 'storybook-request' }
const jordan = {
  user: 'users/jordan',
  name: 'Jordan Lee',
  email: 'jordan@example.com',
  local: false,
}
export const workspace = {
  name: 'workspaces/demo',
  displayName: 'Finance ops',
  state: 'active',
  etag: 'workspace-etag',
  effectiveRole: 'owner',
  currentUser: 'users/jordan',
  profile: jordan,
  mcpEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  connectEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  path: '/workspaces/demo',
}
// Recipes as the catalog receives them from the bundled library.
export const githubRecipe: SourceChoice = {
  id: 'github',
  displayName: 'GitHub',
  description: 'Issues, pull requests, repositories, actions and releases from GitHub.',
  kind: 'openapi',
  url: 'https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.yaml',
  icons: { light: githubIcon },
}
export const stripeRecipe: SourceChoice = {
  id: 'stripe',
  displayName: 'Stripe',
  description: 'Charges, customers, subscriptions, invoices, payouts and balance from Stripe.',
  kind: 'openapi',
  url: 'https://raw.githubusercontent.com/stripe/openapi/refs/heads/master/latest/openapi.spec3.yaml',
  icons: { light: stripeIcon },
}
// A second recipe for one service: the catalog offers both on the GitHub card.
export const githubMcpRecipe: SourceChoice = {
  id: 'github_mcp',
  service: 'github',
  displayName: 'GitHub',
  description: "GitHub's hosted MCP server.",
  kind: 'mcp',
  url: 'https://api.githubcopilot.com/mcp/',
  icons: { light: githubIcon },
}
export const catalogRecipes = [githubRecipe, githubMcpRecipe, stripeRecipe]
