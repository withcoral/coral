import { spawn } from 'node:child_process'

export interface ProcessExit {
  code: number | null
  signal: string | null
}

/** A child process the supervisor owns, whatever API started it. */
export interface ManagedProcess {
  /** Settles once the process has exited. Never rejects; a failed launch is an exit. */
  readonly exited: Promise<ProcessExit>
  /** Asks the process to finish its work and exit. */
  stop(): void
  /** Ends the process without a chance to clean up. */
  kill(): void
  /** The end of the process's combined output, for failure reports. */
  outputTail(): string
}

/** Starts one process with the variables the supervisor adds for this launch. */
export type Launcher<Env extends Record<string, string> = Record<string, string>> = (
  env: Env,
) => ManagedProcess

/** Receives each chunk a process writes, for its log file. */
export type OutputSink = (chunk: string) => void

const OUTPUT_TAIL_LIMIT = 8000
const OUTPUT_DRAIN_MS = 1000

/** Keeps the last few kilobytes of output and forwards every chunk to `sink`. */
export function outputRecorder(sink: OutputSink = () => {}) {
  let tail = ''
  return {
    write(chunk: Buffer | string) {
      const text = chunk.toString()
      sink(text)
      tail = (tail + text).slice(-OUTPUT_TAIL_LIMIT)
    },
    tail: () => tail,
  }
}

export interface SpawnOptions {
  args?: string[]
  cwd: string
  /** The environment every launch inherits; the launch adds its own variables. */
  baseEnv: NodeJS.ProcessEnv
  output?: OutputSink
  /**
   * Run the process as the leader of a new process group and signal the whole
   * group. Use it for wrappers such as npm, which do not forward signals to the
   * server they start. POSIX only.
   */
  processGroup?: boolean
  /**
   * How `stop()` asks the process to exit: SIGTERM, or closing its stdin. A
   * closed stdin works on every platform, and the OS closes it for us when
   * Desktop itself dies. The process must be told to watch its stdin.
   */
  stopBy?: 'signal' | 'stdin'
}

/** Launches `command` as a child process. Never builds a shell command string. */
export function spawnLauncher(command: string, options: SpawnOptions): Launcher {
  return (env) => {
    const output = outputRecorder(options.output)
    const groupKill = options.processGroup === true && process.platform !== 'win32'
    const child = spawn(command, options.args ?? [], {
      cwd: options.cwd,
      env: { ...options.baseEnv, ...env },
      detached: groupKill,
      shell: false,
      stdio: [options.stopBy === 'stdin' ? 'pipe' : 'ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })
    // A process that has exited closes its end; that is not an error here.
    child.stdin?.on('error', () => {})
    child.stdout?.on('data', output.write)
    child.stderr?.on('data', output.write)
    const exited = new Promise<ProcessExit>((resolve) => {
      child.once('error', (error) => {
        output.write(`${error.message}\n`)
        resolve({ code: null, signal: null })
      })
      child.once('exit', (code, signal) => {
        // 'close' follows once piped output has drained, so failure reports
        // keep the last lines. A grandchild holding a pipe must not hold the
        // exit back.
        const settle = () => resolve({ code, signal })
        child.once('close', settle)
        setTimeout(settle, OUTPUT_DRAIN_MS).unref()
      })
    })
    const sendSignal = (signal: NodeJS.Signals) => {
      if (!groupKill || child.pid === undefined) {
        child.kill(signal)
        return
      }
      try {
        process.kill(-child.pid, signal)
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
      }
    }
    return {
      exited,
      stop() {
        if (options.stopBy === 'stdin') child.stdin?.end()
        else sendSignal('SIGTERM')
      },
      kill: () => sendSignal('SIGKILL'),
      outputTail: output.tail,
    }
  }
}
