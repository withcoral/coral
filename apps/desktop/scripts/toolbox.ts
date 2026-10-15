import { createHash } from 'node:crypto'
import { createWriteStream } from 'node:fs'
import { chmod, mkdir, mkdtemp, rename, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { pipeline } from 'node:stream/promises'

// Official release binaries, verified when updating this pin. Architectures
// match electron-builder.config.ts: macOS arm64, Windows and Linux x64.
export const TOOLBOX_RELEASE = {
  version: '1.14.0',
  binaries: {
    darwin: {
      path: 'darwin/arm64/toolbox',
      sha256: 'd1578b3f551b1aa0947c7b377716cd5e8b9448ac5a79fdc1e06dc11a69bbe334',
    },
    win32: {
      path: 'windows/amd64/toolbox.exe',
      sha256: '0426c65989a2cc6a4fbe3caf1145378db5b32a8dd584fc2e0e3d8e93cdc04270',
    },
    linux: {
      path: 'linux/amd64/toolbox',
      sha256: '8ff7c6034d792e2754999e101116ac63b8a9fb926dd2f736065d8e0ef85ead22',
    },
  },
  licenseSha256: '58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd',
}

/** Stage the executable and upstream license without launching Toolbox. */
export async function stageToolbox(
  directory: string,
  platform: NodeJS.Platform,
  release = TOOLBOX_RELEASE,
): Promise<void> {
  if (platform !== 'darwin' && platform !== 'linux' && platform !== 'win32')
    throw new Error(`Toolbox is not packaged for ${platform}`)
  const binary = release.binaries[platform]
  const executable = platform === 'win32' ? 'toolbox.exe' : 'toolbox'
  await mkdir(dirname(directory), { recursive: true })
  const scratch = await mkdtemp(join(dirname(directory), '.toolbox-'))
  try {
    await download(
      `https://storage.googleapis.com/mcp-toolbox-for-databases/v${release.version}/${binary.path}`,
      binary.sha256,
      join(scratch, executable),
    )
    await chmod(join(scratch, executable), 0o755)
    await download(
      `https://raw.githubusercontent.com/googleapis/mcp-toolbox/v${release.version}/LICENSE`,
      release.licenseSha256,
      join(scratch, 'LICENSE'),
    )
    await writeFile(join(scratch, 'VERSION'), `${release.version}\n`)
    // Keep the previous staged copy intact if a download or checksum fails.
    await rm(directory, { recursive: true, force: true })
    await rename(scratch, directory)
  } finally {
    await rm(scratch, { recursive: true, force: true })
  }
  console.log(`Staged MCP Toolbox ${release.version} at ${directory}`)
}

async function download(url: string, expectedHash: string, destination: string): Promise<void> {
  const response = await fetch(url, { signal: AbortSignal.timeout(300_000) })
  if (!response.ok || !response.body) throw new Error(`Downloading ${url}: HTTP ${response.status}`)
  const hash = createHash('sha256')
  await pipeline(
    response.body,
    async function* (source) {
      for await (const chunk of source) {
        hash.update(chunk)
        yield chunk
      }
    },
    createWriteStream(destination),
  )
  if (hash.digest('hex') !== expectedHash) throw new Error(`SHA-256 mismatch for ${url}`)
}
