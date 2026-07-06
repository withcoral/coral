import { highlightSQL } from '@/lib/sql-highlight'

import * as s from '../traces-page.css'

export function SqlCode({ sql, inline = false }: { sql: string; inline?: boolean }) {
  return (
    <code
      className={inline ? s.sqlInlineCode : undefined}
      dangerouslySetInnerHTML={{ __html: highlightSQL(sql) }}
    />
  )
}
