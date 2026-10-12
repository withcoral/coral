import { closeSync, mkdirSync, openSync, renameSync, statSync, writeSync } from 'node:fs'
import { join } from 'node:path'

import type { OutputSink } from './process'

const ROTATE_BYTES = 5 * 1024 * 1024

/**
 * Opens `<dir>/<name>.log` for appending. Before the file would pass 5 MB, at
 * launch or while the process runs, it moves to `<name>.1.log`, replacing the
 * previous one. `echo` also receives every chunk, for a developer's terminal.
 *
 * Writes are synchronous: output arrives in small chunks, and a file that is
 * closed before it is renamed rotates the same way on every platform.
 */
export function logSink(dir: string, name: string, echo?: OutputSink): OutputSink {
  mkdirSync(dir, { recursive: true, mode: 0o700 })
  const file = join(dir, `${name}.log`)
  const rotated = join(dir, `${name}.1.log`)
  let size = 0
  try {
    size = statSync(file).size
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
  }
  let failed = false
  // A full disk must not take the app down; the output then reaches only `echo`.
  const report = (error: unknown) => {
    if (failed) return
    failed = true
    console.error(`[coral-desktop] ${file}: ${(error as Error).message}`)
  }
  const rotate = () => {
    size = 0
    try {
      renameSync(file, rotated)
    } catch (error) {
      report(error)
    }
  }
  const open = () => {
    try {
      return openSync(file, 'a', 0o600)
    } catch (error) {
      report(error)
      return undefined
    }
  }
  if (size > ROTATE_BYTES) rotate()
  let fd = open()
  return (chunk) => {
    echo?.(chunk)
    const bytes = Buffer.byteLength(chunk)
    if (size > 0 && size + bytes > ROTATE_BYTES) {
      if (fd !== undefined) closeSync(fd)
      rotate()
      fd = open()
    }
    if (fd === undefined) return
    try {
      writeSync(fd, chunk)
      size += bytes
    } catch (error) {
      report(error)
    }
  }
}
