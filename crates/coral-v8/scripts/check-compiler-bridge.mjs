// compiler.rs joins the compiler scripts into one function scope, so a file's
// top-level declaration can be another file's dependency. This lints the
// joined bridge for unused bindings and reports each one in its own file.
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const RUST_ESCAPES = { n: '\n', r: '\r', t: '\t', 0: '\0', '\\': '\\', '"': '"', "'": "'" };

// Rebuilds COMPILER_BRIDGE from its concat! arguments, so compiler.rs alone
// decides the bridge's text.
function compilerBridge() {
  const bridge = /const COMPILER_BRIDGE: &str = concat!\(([\s\S]*?)\n\);/.exec(
    readFileSync('src/compiler.rs', 'utf8'),
  );
  if (!bridge) throw new Error('src/compiler.rs: COMPILER_BRIDGE is not a concat!');
  const argument = /\s*(?:"((?:[^"\\]|\\.)*)"|include_str!\("([^"]+)"\))\s*(?:,|$)/y;
  let source = '';
  const starts = [];
  while (argument.lastIndex < bridge[1].length) {
    const match = argument.exec(bridge[1]);
    if (!match) throw new Error('src/compiler.rs: COMPILER_BRIDGE has an unreadable argument');
    const [, literal, included] = match;
    if (included === undefined) {
      source += literal.replace(/\\(.)/g, (escape, character) => {
        if (!(character in RUST_ESCAPES)) throw new Error(`unsupported escape ${escape}`);
        return RUST_ESCAPES[character];
      });
    } else {
      const file = `src/${included}`;
      starts.push({ file, line: source.split('\n').length });
      source += readFileSync(file, 'utf8');
    }
  }
  if (starts.length === 0) throw new Error('src/compiler.rs: COMPILER_BRIDGE names no scripts');
  return { source, starts };
}

const { source, starts } = compilerBridge();
// Run oxlint's entry point with Node: npm's Windows .cmd shim needs a shell.
const oxlint = fileURLToPath(new URL('../node_modules/oxlint/bin/oxlint', import.meta.url));
const directory = mkdtempSync(join(tmpdir(), 'coral-compiler-bridge-'));
try {
  const joinedPath = join(directory, 'compiler_bridge.js');
  writeFileSync(joinedPath, source);
  const result = spawnSync(
    process.execPath,
    [oxlint, '-c', '.oxlintrc.json', '-A', 'all', '-D', 'no-unused-vars', '-f', 'unix', joinedPath],
    { encoding: 'utf8' },
  );
  if (result.error) throw result.error;
  const findings = result.stdout
    .split('\n')
    .map((line) => /^.*compiler_bridge\.js:(\d+):(\d+): (.*)$/.exec(line))
    .filter(Boolean)
    .map(([, line, column, message]) => {
      const start = starts.findLast((entry) => entry.line <= Number(line));
      return `${start.file}:${Number(line) - start.line + 1}:${column}: ${message}`;
    });
  for (const finding of findings) console.error(finding);
  if (findings.length === 0 && result.status !== 0) console.error(result.stdout, result.stderr);
  if (findings.length > 0 || result.status !== 0) process.exitCode = 1;
} finally {
  rmSync(directory, { recursive: true, force: true });
}
