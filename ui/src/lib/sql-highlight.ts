const SQL_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
  'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL', 'CROSS', 'ON',
  'GROUP', 'BY', 'ORDER', 'ASC', 'DESC', 'HAVING', 'LIMIT', 'OFFSET',
  'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE', 'DROP',
  'ALTER', 'TABLE', 'INDEX', 'AS', 'DISTINCT',
  'NULL', 'IS', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN',
  'ELSE', 'END', 'UNION', 'ALL', 'EXISTS', 'WITH', 'RECURSIVE',
  'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'DEFAULT', 'CONSTRAINT',
])

const SQL_FUNCTIONS = new Set([
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CAST',
  'UPPER', 'LOWER', 'TRIM', 'LENGTH', 'SUBSTRING', 'REPLACE',
  'NOW', 'DATE', 'EXTRACT', 'ROUND', 'ABS', 'FLOOR', 'CEIL',
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD',
  'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE', 'REAL',
  'VARCHAR', 'CHAR', 'TEXT', 'BOOLEAN', 'BOOL',
  'DECIMAL', 'NUMERIC', 'TIMESTAMP', 'TIME', 'INTERVAL',
  'SERIAL', 'UUID', 'JSON', 'JSONB', 'ARRAY',
])

const BREAK_BEFORE = /\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|FULL JOIN|CROSS JOIN|GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET|UNION ALL|UNION|WITH|INSERT INTO|VALUES|UPDATE|SET|DELETE FROM)\b/gi

export function formatSQL(sql: string): string {
  if (sql.includes('\n')) return sql
  return sql
    .replace(/\s+/g, ' ')
    .trim()
    .replace(BREAK_BEFORE, '\n$1')
    .trim()
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

export function highlightSQL(sql: string): string {
  return escapeHtml(sql).replace(/(--[^\n]*)|(&#39;[^&]*(?:&(?!#39;)[^&]*)*&#39;)|(&quot;[\w]+&quot;)|(\b[A-Za-z_]\w*\b)|(\d+(?:\.\d+)?)/g, (match, comment, str, quoted, word, num) => {
    if (comment) return `<span class="sql-comment">${match}</span>`
    if (str) return `<span class="sql-string">${match}</span>`
    if (quoted) return `<span class="sql-identifier">${match}</span>`
    if (word && SQL_KEYWORDS.has(word.toUpperCase())) return `<span class="sql-keyword">${word}</span>`
    if (word && SQL_FUNCTIONS.has(word.toUpperCase())) return `<span class="sql-function">${word}</span>`
    if (num) return `<span class="sql-number">${match}</span>`
    return match
  })
}
