import type { PropsWithChildren } from 'react'
import { createContext, useContext } from 'react'

import type { Column } from './columns'
import type { TableLayout } from './constants'

const ColumnsContext = createContext<{ columns: readonly Column[]; layout: TableLayout }>({
  columns: [],
  layout: 'auto',
})

export function ColumnsProvider({
  children,
  columns,
  layout,
}: PropsWithChildren<{ columns: readonly Column[]; layout: TableLayout }>) {
  return <ColumnsContext.Provider value={{ columns, layout }}>{children}</ColumnsContext.Provider>
}

/** The descriptors `Table.Container` was given, for `Table.Head` to render. */
export function useColumns(): readonly Column[] {
  return useContext(ColumnsContext).columns
}

export function useTableLayout(): TableLayout {
  return useContext(ColumnsContext).layout
}
