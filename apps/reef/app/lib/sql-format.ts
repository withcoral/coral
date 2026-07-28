const BREAK_BEFORE =
  /\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|FULL JOIN|CROSS JOIN|GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET|UNION ALL|UNION|WITH|INSERT INTO|VALUES|UPDATE|SET|DELETE FROM)\b/gi

export function formatSQL(sql: string): string {
  if (sql.includes('\n')) return sql
  return sql.replace(/\s+/g, ' ').trim().replace(BREAK_BEFORE, '\n$1').trim()
}
