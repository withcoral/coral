import type { WebMcpClient } from './mcp-clients'

const ADD_MCP_VERSION = '1.11.0'

function installerNodeScript(): string {
  return `import { agents, getAgentTypes, listInstalledServers, upsertServer } from 'add-mcp'

const [client, coralBin] = process.argv.slice(2)
if (!client || !coralBin || !getAgentTypes().includes(client)) {
  throw new Error('Unsupported MCP client.')
}
if (!agents[client].supportedTransports.includes('stdio')) {
  throw new Error(client + ' does not support stdio MCP servers.')
}

const [installed] = await listInstalledServers({ agents: [client], global: true })
if (installed?.servers.some(({ serverName }) => serverName === 'coral')) {
  throw new Error(
    installed.displayName + ' already has a global MCP server named "coral". Refusing to replace it.',
  )
}

const result = upsertServer(
  client,
  'coral',
  { args: ['mcp-stdio'], command: coralBin },
  { local: false },
)
if (!result.success) throw new Error(result.error ?? ('Could not configure ' + client + '.'))
console.log('Configured Coral for ' + client + '. Restart the client to load the MCP server.')
`
}

export function mcpInstallerScript(client: WebMcpClient): string {
  // The client comes from the allowlisted catalog rather than request input.
  return `#!/usr/bin/env sh
set -eu

client=${JSON.stringify(client.id)}

fail() {
  printf '%s\\n' "Error: $1" >&2
  exit 1
}

case "$(uname -s)" in
  Darwin|Linux) ;;
  *) fail 'This installer currently supports macOS and Linux only.' ;;
esac

command -v node >/dev/null 2>&1 || fail 'Node.js 18 or newer is required to configure this MCP client.'
command -v npm >/dev/null 2>&1 || fail 'npm is required to configure this MCP client.'
coral_bin="$(command -v coral || true)"
[ -n "$coral_bin" ] || fail 'Install Coral and ensure coral is on PATH before configuring an MCP client.'

temp_dir="$(mktemp -d)"
trap 'rm -rf "$temp_dir"' EXIT HUP INT TERM
npm install --ignore-scripts --no-save --package-lock=false --prefix "$temp_dir" "add-mcp@${ADD_MCP_VERSION}" >/dev/null

cat > "$temp_dir/install.mjs" <<'NODE'
${installerNodeScript()}NODE

node "$temp_dir/install.mjs" "$client" "$coral_bin"
`
}

export function mcpInstallerPowerShellScript(client: WebMcpClient): string {
  return `$ErrorActionPreference = 'Stop'
$client = ${JSON.stringify(client.id)}

function Fail([string]$Message) {
  throw "Error: $Message"
}

if ($null -eq (Get-Command node -ErrorAction SilentlyContinue)) {
  Fail 'Node.js 18 or newer is required to configure this MCP client.'
}
if ($null -eq (Get-Command npm -ErrorAction SilentlyContinue)) {
  Fail 'npm is required to configure this MCP client.'
}
$coralCommand = Get-Command coral -ErrorAction SilentlyContinue
if ($null -eq $coralCommand) {
  Fail 'Install Coral and ensure coral is on PATH before configuring an MCP client.'
}

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ('coral-mcp-' + [guid]::NewGuid())
try {
  New-Item -ItemType Directory -Path $tempDir | Out-Null
  & npm install --ignore-scripts --no-save --package-lock=false --prefix $tempDir "add-mcp@${ADD_MCP_VERSION}" | Out-Null
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$nodeScript = @'
${installerNodeScript()}'@
  Set-Content -Encoding utf8 -NoNewline -Value $nodeScript (Join-Path $tempDir 'install.mjs')

  & node (Join-Path $tempDir 'install.mjs') $client $coralCommand.Source
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
  Remove-Item -Force -Recurse $tempDir -ErrorAction SilentlyContinue
}
`
}
