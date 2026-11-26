/** A count with its noun, such as "1 function" or "1,204 operations". */
export function countLabel(count: number, noun: string) {
  return `${count.toLocaleString('en-US')} ${noun}${count === 1 ? '' : 's'}`
}
