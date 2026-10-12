import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const exec = promisify(execFile)

// Run once at fixture bootstrap; workers receive the resolved choice rather
// than rediscovering an absent optional tool for every browser recording.
export async function detectFfmpeg(binary: string) {
  try {
    await exec(binary, ['-version'], { timeout: 5000, killSignal: 'SIGKILL' })
    return { binary, notice: undefined }
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    return {
      binary: null,
      notice:
        code === 'ENOENT'
          ? 'ffmpeg not found; keeping WebM recordings. Install ffmpeg to enable MP4.'
          : `ffmpeg check failed (${code ?? 'unknown error'}); keeping WebM recordings. Check E2E_FFMPEG_BINARY.`,
    }
  }
}
