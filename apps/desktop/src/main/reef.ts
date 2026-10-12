import { utilityProcess } from 'electron'

import { outputRecorder, type Launcher, type OutputSink, type ProcessExit } from './process'

/**
 * Runs Reef's bundled server in an Electron utility process, on Electron's own
 * Node, so a packaged app needs no separate Node binary and no RunAsNode fuse.
 */
export function utilityLauncher(
  modulePath: string,
  options: { cwd: string; baseEnv: NodeJS.ProcessEnv; output?: OutputSink },
): Launcher {
  return (env) => {
    const output = outputRecorder(options.output)
    const child = utilityProcess.fork(modulePath, [], {
      cwd: options.cwd,
      env: { ...options.baseEnv, ...env },
      serviceName: 'Coral web server',
      stdio: 'pipe',
    })
    child.stdout?.on('data', output.write)
    child.stderr?.on('data', output.write)
    const exited = new Promise<ProcessExit>((resolve) =>
      child.once('exit', (code) => resolve({ code, signal: null })),
    )
    return {
      exited,
      // kill() sends SIGTERM, which Reef's server handles by closing. On
      // Windows it ends the process at once; Reef keeps no durable state.
      stop: () => child.kill(),
      kill() {
        if (child.pid === undefined) return
        try {
          process.kill(child.pid, 'SIGKILL')
        } catch (error) {
          // It exited between the timeout and the kill.
          if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
        }
      },
      outputTail: output.tail,
    }
  }
}
