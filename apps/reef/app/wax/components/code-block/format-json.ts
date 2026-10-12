/** Format JSON by changing whitespace only, never by reserializing parsed values.
 * Preserve precision, duplicate keys, escapes, and embedded JSON strings.
 * Pack scalar arrays into readable lines. Structured arrays remain indented.
 * Invalid JSON is unchanged. Bound indentation, never the evidence itself.
 */
export function formatJson(source: string): string {
  try {
    JSON.parse(source)
  } catch {
    return source
  }
  const tokens = source.match(/"(?:\\.|[^"\\])*"|[{}[\],:]|[^\s{}[\],:]+/g) ?? []
  const output: string[] = []
  let depth = 0
  let column = 0
  const append = (text: string) => {
    output.push(text)
    column += text.length
  }
  const line = () => {
    const indent = '  '.repeat(Math.min(depth, 40))
    output.push('\n', indent)
    column = indent.length
  }
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]
    if (token === '[') {
      let end = index + 1
      while (end < tokens.length && !['[', '{', ']'].includes(tokens[end])) end++
      if (tokens[end] === ']') {
        append('[')
        depth++
        for (let item = index + 1; item < end; item++) {
          if (tokens[item] === ',') {
            append(',')
            // Reserve one column for the following comma or closing bracket.
            if (column + 1 + tokens[item + 1].length + 1 > 80) line()
            else append(' ')
          } else append(tokens[item])
        }
        depth--
        append(']')
        index = end
        continue
      }
    }
    switch (token) {
      case '{':
      case '[':
        append(token)
        depth++
        if (tokens[index + 1] !== '}' && tokens[index + 1] !== ']') line()
        break
      case '}':
      case ']':
        depth--
        if (tokens[index - 1] !== '{' && tokens[index - 1] !== '[') line()
        append(token)
        break
      case ',':
        append(token)
        line()
        break
      case ':':
        append(': ')
        break
      default:
        append(token)
    }
  }
  return output.join('')
}
