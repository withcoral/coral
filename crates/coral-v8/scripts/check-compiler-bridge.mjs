// compiler.rs joins the compiler scripts into one function scope, so a file's
// top-level declaration can be another file's dependency. This lints the
// joined bridge for unused bindings and reports each one in its own file.
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const bridge = /const COMPILER_BRIDGE: &str = concat!\(([\s\S]*?)\n\);/.exec(
  readFileSync('src/compiler.rs', 'utf8'),
);
const files = [...(bridge?.[1] ?? '').matchAll(/include_str!\("(js\/compiler\/[^"]+)"\)/g)].map(
  (match) => `src/${match[1]}`,
);
if (files.length === 0) {
  console.error('src/compiler.rs: COMPILER_BRIDGE names no compiler scripts');
  process.exit(1);
}

// Mirror compiler.rs: a two-line prologue, then each file and a newline.
let joined = '"use strict";\n(() => {\n';
const starts = [];
for (const file of files) {
  starts.push({ file, line: joined.split('\n').length });
  joined += `${readFileSync(file, 'utf8')}\n`;
}
joined += '})();\n';

const directory = mkdtempSync(join(tmpdir(), 'coral-compiler-bridge-'));
try {
  const joinedPath = join(directory, 'compiler_bridge.js');
  writeFileSync(joinedPath, joined);
  const result = spawnSync(
    'oxlint',
    ['-c', '.oxlintrc.json', '-A', 'all', '-D', 'no-unused-vars', '-f', 'unix', joinedPath],
    { encoding: 'utf8' },
  );
  if (result.error) throw result.error;
  const findings = result.stdout
    .split('\n')
    .map((line) => /^[^:]+:(\d+):(\d+): (.*)$/.exec(line))
    .filter(Boolean)
    .map(([, line, column, message]) => {
      const start = starts.findLast((entry) => entry.line <= Number(line));
      return `${start.file}:${Number(line) - start.line + 1}:${column}: ${message}`;
    });
  for (const finding of findings) console.error(finding);
  if (findings.length > 0 || result.status !== 0) {
    if (findings.length === 0) console.error(result.stdout, result.stderr);
    process.exit(1);
  }
} finally {
  rmSync(directory, { recursive: true, force: true });
}
