import { createContext, useContext } from 'react'

/**
 * Whether Reef serves a local deployment: sign-in is disabled, and the
 * implicit local User is the only person. The app shell provides it from the
 * request's auth mode. Views outside the shell count as signed in.
 */
export const LocalDeploymentContext = createContext(false)

export function useLocalDeployment() {
  return useContext(LocalDeploymentContext)
}
