import { describe, expect, it } from 'vitest'
import { formatJson } from './format-json'

describe('lossless JSON display', () => {
  it('indents objects and arrays without changing number or string tokens', () => {
    expect(
      formatJson(
        '{"id":9007199254740993,"amount":1.2300,"negativeZero":-0,"items":[true,null,{}]}',
      ),
    ).toBe(`{
  "id": 9007199254740993,
  "amount": 1.2300,
  "negativeZero": -0,
  "items": [
    true,
    null,
    {}
  ]
}`)
  })
  it('packs scalar arrays without changing exact values or flattening structured items', () => {
    expect(
      formatJson(
        '{"fields":["id","title","url"],"values":[9007199254740993,1.2300,-0,1e10000,true,null]}',
      ),
    ).toBe(`{
  "fields": ["id", "title", "url"],
  "values": [9007199254740993, 1.2300, -0, 1e10000, true, null]
}`)
  })
  it('wraps long scalar arrays at item boundaries without dropping or rewriting string tokens', () => {
    const source =
      '{"fields":["identifier","title","url","status","statusType","priority","dueDate","updatedAt","project","description"]}'
    const formatted = formatJson(source)
    expect(formatted.split('\n').length).toBeLessThan(7)
    expect(formatted.split('\n').every((line) => line.length <= 80)).toBe(true)
    expect(formatted.match(/"(?:\\.|[^"\\])*"|[^\s{}[\],:]+/g)).toEqual(
      source.match(/"(?:\\.|[^"\\])*"|[^\s{}[\],:]+/g),
    )
  })
  it('preserves embedded JSON as a string, escaped quotes, key order and duplicate keys', () => {
    const source = String.raw`{"text":"{\"n\":9007199254740993}","key":"braces } [ and slash \\","key":"\u0061"}`
    expect(formatJson(source)).toBe(String.raw`{
  "text": "{\"n\":9007199254740993}",
  "key": "braces } [ and slash \\",
  "key": "\u0061"
}`)
  })
  it.each(['null', '[]', '{}', '9007199254740993', '1e10000', '"a string"'])(
    'keeps %s intact',
    (value) => {
      expect(formatJson(value)).toBe(value)
    },
  )
  it.each(['', 'not JSON', '{"broken":', '{"a":1,}', 'console.info("raw source");'])(
    'does not rewrite invalid JSON or text: %s',
    (value) => {
      expect(formatJson(value)).toBe(value)
    },
  )
  it('bounds visual indentation without dropping deeply nested evidence', () => {
    const source = '['.repeat(500) + '0' + ']'.repeat(500)
    const formatted = formatJson(source)
    expect(formatted.replace(/\s/g, '')).toBe(source)
    expect(
      Math.max(...formatted.split('\n').map((line) => /^ */.exec(line)![0].length)),
    ).toBeLessThanOrEqual(80)
  })
})
