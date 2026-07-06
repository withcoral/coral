import { createContext, useContext } from 'react'

interface AnchorContextValue {
  inputGroupAnchor: HTMLDivElement | null
  setInputGroupAnchor: (anchor: HTMLDivElement | null) => void
}

export const AnchorContext = createContext<AnchorContextValue | null>(null)

export function useAnchorContext() {
  return useContext(AnchorContext)
}
