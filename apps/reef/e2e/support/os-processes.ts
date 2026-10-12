// What the OS reports about processes Reel did not start itself, such as the
// runtime and Reef that Desktop supervises. Reel reads the OS process table and
// socket table, so Desktop needs no test API.
import { execFile } from 'node:child_process'
import { readdir, readFile, readlink } from 'node:fs/promises'
import { promisify } from 'node:util'

const exec = promisify(execFile)
// Process tables on busy hosts run to megabytes.
const MAX_OUTPUT = 16 * 1024 * 1024

export interface ProcessRow {
  pid: number
  ppid: number
  /** The command line, as one string. */
  args: string
}

/** Every process started, directly or not, by `root`, without `root` itself. */
export async function descendantProcesses(root: number): Promise<ProcessRow[]> {
  const rows = await processTable()
  const descendants = new Set([root])
  for (let grown = true; grown;) {
    grown = false
    for (const row of rows)
      if (descendants.has(row.ppid) && !descendants.has(row.pid)) {
        descendants.add(row.pid)
        grown = true
      }
  }
  return rows.filter((row) => row.pid !== root && descendants.has(row.pid))
}

export interface Listener {
  pid: number
  /** The bound address as the OS prints it, for example `127.0.0.1` or `::1`. */
  address: string
  port: number
}

/** The TCP sockets that `pids` listen on. */
export async function tcpListeners(pids: readonly number[]): Promise<Listener[]> {
  if (pids.length === 0) return []
  if (process.platform === 'win32') {
    const rows = await powershellRows<{
      LocalAddress: string
      LocalPort: number
      OwningProcess: number
    }>(
      'Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Select-Object LocalAddress,LocalPort,OwningProcess',
    )
    return rows
      .filter((row) => pids.includes(row.OwningProcess))
      .map((row) => ({ pid: row.OwningProcess, address: row.LocalAddress, port: row.LocalPort }))
  }
  if (process.platform === 'linux') return linuxListeners(pids)
  throw new Error(`Reel cannot list listeners on ${process.platform}`)
}

async function processTable(): Promise<ProcessRow[]> {
  if (process.platform === 'win32') {
    const rows = await powershellRows<{
      ProcessId: number
      ParentProcessId: number
      CommandLine: string | null
    }>('Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,CommandLine')
    return rows.map((row) => ({
      pid: row.ProcessId,
      ppid: row.ParentProcessId,
      args: row.CommandLine ?? '',
    }))
  }
  const { stdout } = await exec('ps', ['-eo', 'pid=,ppid=,args='], { maxBuffer: MAX_OUTPUT })
  return stdout
    .split('\n')
    .map((line) => /^\s*(\d+)\s+(\d+)\s+(.*)$/.exec(line))
    .filter((match) => match !== null)
    .map(([, pid, ppid, args]) => ({ pid: Number(pid), ppid: Number(ppid), args }))
}

/** Runs a PowerShell pipeline and parses its objects, always as an array. */
async function powershellRows<Row>(pipeline: string): Promise<Row[]> {
  const { stdout } = await exec(
    'powershell.exe',
    [
      '-NoProfile',
      '-NonInteractive',
      '-Command',
      // Windows PowerShell writes redirected output in the OEM code page, which
      // breaks non-ASCII paths and user names.
      `[Console]::OutputEncoding = [Text.Encoding]::UTF8; ConvertTo-Json -Compress -InputObject @(${pipeline})`,
    ],
    { maxBuffer: MAX_OUTPUT },
  )
  return JSON.parse(stdout) as Row[]
}

/** Matches each listening socket in the namespace's TCP tables to the process holding it. */
async function linuxListeners(pids: readonly number[]): Promise<Listener[]> {
  const owners = new Map<string, number>()
  for (const pid of pids) {
    const fds = await readdir(`/proc/${pid}/fd`).catch(() => [])
    for (const fd of fds) {
      const link = await readlink(`/proc/${pid}/fd/${fd}`).catch(() => '')
      const inode = /^socket:\[(\d+)\]$/.exec(link)?.[1]
      if (inode) owners.set(inode, pid)
    }
  }
  const listeners: Listener[] = []
  for (const table of ['/proc/net/tcp', '/proc/net/tcp6']) {
    const rows = (await readFile(table, 'utf8').catch(() => '')).split('\n').slice(1)
    for (const row of rows) {
      const fields = row.trim().split(/\s+/)
      // Columns: sl, local address, remote address, state, …, inode (10th).
      const [, local, , state] = fields
      const pid = owners.get(fields[9])
      // 0A is TCP_LISTEN.
      if (state !== '0A' || pid === undefined) continue
      const [address, port] = local.split(':')
      listeners.push({ pid, address: procAddress(address), port: Number.parseInt(port, 16) })
    }
  }
  return listeners
}

/** Decodes /proc's hex address: 32-bit words in host byte order, little-endian here. */
function procAddress(hex: string): string {
  const words = hex.match(/.{8}/g)!.map((word) => word.match(/../g)!.toReversed().join(''))
  if (words.length === 1)
    return words[0]
      .match(/../g)!
      .map((byte) => Number.parseInt(byte, 16))
      .join('.')
  // URL parsing writes an IPv6 address in its canonical short form.
  const full = words.join('').match(/.{4}/g)!.join(':')
  return new URL(`http://[${full}]/`).hostname.slice(1, -1)
}
