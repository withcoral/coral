function serverOnlyCoralClient(): never {
  throw new Error('server-side Coral clients are unavailable in Reef browser tests')
}

export const catalogClientForRequest = serverOnlyCoralClient
export const coralEndpointForRequest = serverOnlyCoralClient
export const functionClientForRequest = serverOnlyCoralClient
export const queryClientForRequest = serverOnlyCoralClient
export const sourceClientForRequest = serverOnlyCoralClient
export const traceClientForRequest = serverOnlyCoralClient
export const workspaceClientForRequest = serverOnlyCoralClient
