import type { PropsWithChildren } from 'react'
import { createContext, useContext } from 'react'

import type { Column } from './columns'

const ColumnsContext = createContext<readonly Column[]>([])

export function ColumnsProvider({
  children,
  columns,
}: PropsWithChildren<{ columns: readonly Column[] }>) {
  return <ColumnsContext.Provider value={columns}>{children}</ColumnsContext.Provider>
}

/** The descriptors `Table.Container` was given, for `Table.Head` to render. */
export function useColumns(): readonly Column[] {
  return useContext(ColumnsContext)
}
