import { createContext, useContext } from 'react'

const NameContext = createContext<string | null>(null)

/** Ends every page title below it with one name, such as the product's. */
export const PageTitleRoot = NameContext.Provider

/** Titles the browser tab. React moves the element into the document head. */
export function PageTitle({ children }: { children: string }) {
  const name = useContext(NameContext)
  return <title>{name ? `${children} · ${name}` : children}</title>
}
