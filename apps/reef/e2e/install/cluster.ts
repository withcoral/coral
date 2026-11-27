// Sets up the helm-install suite's cluster on the host, before the Reel container
// starts, and takes it down after: a kind cluster with Envoy Gateway, a
// test-only Postgres and an Install from charts/coral. The Reel container then
// joins kind's Docker network and drives the Install from outside, as a user
// and an MCP client would.
import { execFile, spawn } from 'node:child_process'
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'

import { repoRoot } from '../support/config.ts'
import { FIXTURE_CLIENT_ID } from '../support/oidc-fixture.ts'
import {
  INSTALL_HOSTS,
  INSTALL_TLS_DIRECTORY,
  INSTALL_TLS_FILES,
  type InstallSecrets,
} from '../support/install.ts'

// The newest Kubernetes minor the chart supports, and the components that
// serve it. Each is pinned; bump them together with the chart's support window:
// kube_versions in scripts/check-chart.sh and KUBECTL_VERSION in reel.yml.
const KIND_NODE_IMAGE =
  'kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5'
const CLOUD_PROVIDER_KIND_IMAGE =
  'registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0@sha256:1b97632e5dfec8f83c7e970cf18cedb9e6b707e272011d712cabf6ab59d323c9'
const ENVOY_GATEWAY_CHART = 'oci://docker.io/envoyproxy/gateway-helm'
const ENVOY_GATEWAY_VERSION = 'v1.10.0'
// The image Reel's own Postgres uses (scripts/postgres.ts), with pgvector.
const POSTGRES_IMAGE =
  'pgvector/pgvector:0.8.2-pg17-trixie@sha256:5c97c57367a485a8e99389548db67d441ab1a878f5492c3df04989f34ecf3c75'

const NAMESPACE = 'coral'
const POSTGRES_HOST = `postgres.${NAMESPACE}.svc.cluster.local`

const run = promisify(execFile)

export interface Install {
  /** The Reel container's extra `docker run` arguments: network, names, the CA mount. */
  containerArgs: string[]
  /** Collects the cluster's evidence, then deletes everything this run made. */
  stop(): Promise<void>
}

export interface InstallOptions {
  /** The images to install, already built: `make docker-build reef-docker-build`. */
  coralImage: string
  webImage: string
  /** Leave the cluster running after the run, for a person to look at. */
  keep: boolean
  /** Where the cluster's objects, events and pod logs go when the run ends. */
  evidence: string
  /** Stops setup when the run is cancelled. Cleanup runs whatever happens. */
  signal: AbortSignal
}

/** Creates the cluster and installs Coral into it. */
export async function startInstall(options: InstallOptions): Promise<Install> {
  await checkPrerequisites(options)
  const name = `reel-${randomUUID().slice(0, 8)}`
  const work = await mkdtemp(join(tmpdir(), 'reel-install-'))
  const tls = join(work, 'tls')
  const kubeconfig = join(work, 'kubeconfig')
  // Only what kind, kubectl, Helm and Docker need: no developer settings.
  const env = {
    ...Object.fromEntries(
      ['PATH', 'HOME', 'TMPDIR', 'DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_CONFIG'].flatMap((key) =>
        process.env[key] ? [[key, process.env[key]]] : [],
      ),
    ),
    KUBECONFIG: kubeconfig,
  }
  const made = { cluster: false, cloudProvider: false }
  // Setup stops when the run is cancelled; evidence and cleanup do not.
  const command = async (
    signal: AbortSignal | undefined,
    program: string,
    args: string[],
    input?: string,
  ) => {
    if (input === undefined)
      return (await run(program, args, { env, maxBuffer: 1 << 26, signal })).stdout
    return pipe(program, args, input, env, signal)
  }
  const exec = (program: string, args: string[], input?: string) =>
    command(options.signal, program, args, input)
  const kubectl = (args: string[], input?: string) => exec('kubectl', args, input)
  const apply = (manifest: string) => kubectl(['apply', '-f', '-'], manifest)

  const stop = async () => {
    try {
      if (made.cluster)
        await collectEvidence((args) => command(undefined, 'kubectl', args), options.evidence)
    } finally {
      if (options.keep) {
        console.log(`Reel kept the install cluster ${name}; its kubeconfig is ${kubeconfig}`)
      } else {
        if (made.cloudProvider) {
          await run('docker', ['rm', '-f', `${name}-ccm`]).catch(() => {})
          // cloud-provider-kind's load balancers outlive it; they carry the cluster's name.
          const balancers = (
            await run('docker', [
              'ps',
              '--all',
              '--quiet',
              '--filter',
              `label=io.x-k8s.cloud-provider-kind.cluster=${name}`,
            ]).catch(() => ({ stdout: '' }))
          ).stdout
            .split('\n')
            .filter(Boolean)
          if (balancers.length) await run('docker', ['rm', '-f', ...balancers]).catch(() => {})
        }
        if (made.cluster)
          await run('kind', ['delete', 'cluster', '--name', name]).catch((error) => {
            console.error(`Reel could not delete the kind cluster ${name}: ${String(error)}`)
          })
        await rm(work, { recursive: true, force: true })
      }
    }
  }

  try {
    const secrets = await certificates(tls)
    console.log(`Reel install: creating the kind cluster ${name}`)
    made.cluster = true
    await exec('kind', [
      'create',
      'cluster',
      '--name',
      name,
      '--image',
      KIND_NODE_IMAGE,
      '--kubeconfig',
      kubeconfig,
      '--wait',
      '180s',
    ])
    // The workloads run on the one node, so load balancers must reach it.
    await kubectl([
      'label',
      'node',
      `${name}-control-plane`,
      'node.kubernetes.io/exclude-from-external-load-balancers-',
    ])
    // Envoy Gateway serves the Gateway; cloud-provider-kind only gives its
    // Service an address on kind's Docker network.
    made.cloudProvider = true
    await exec('docker', [
      'run',
      '--detach',
      '--name',
      `${name}-ccm`,
      '--network',
      'kind',
      '--volume',
      '/var/run/docker.sock:/var/run/docker.sock',
      CLOUD_PROVIDER_KIND_IMAGE,
      '--gateway-channel',
      'disabled',
    ])
    console.log('Reel install: loading the images')
    await exec('kind', [
      'load',
      'docker-image',
      '--name',
      name,
      options.coralImage,
      options.webImage,
    ])
    console.log('Reel install: installing Envoy Gateway')
    await exec('helm', [
      'install',
      'eg',
      ENVOY_GATEWAY_CHART,
      '--version',
      ENVOY_GATEWAY_VERSION,
      '--namespace',
      'envoy-gateway-system',
      '--create-namespace',
      '--wait',
      '--timeout',
      '5m',
    ])
    await kubectl(['create', 'namespace', NAMESPACE])
    await kubectl([
      'create',
      'secret',
      'tls',
      'gateway-tls',
      '--namespace',
      NAMESPACE,
      `--cert=${join(tls, 'gateway.pem')}`,
      `--key=${join(tls, 'gateway.key')}`,
    ])
    await apply(gatewayManifest())
    const address = await gatewayAddress(kubectl)
    // Pods reach the Install's hosts through the Gateway, as the browser does.
    await pointHostsAt(kubectl, address)
    console.log('Reel install: starting Postgres')
    await kubectl([
      'create',
      'secret',
      'tls',
      'postgres-tls',
      '--namespace',
      NAMESPACE,
      `--cert=${join(tls, 'postgres.pem')}`,
      `--key=${join(tls, 'postgres.key')}`,
    ])
    const postgresPassword = randomBytes(24).toString('hex')
    await apply(postgresManifest(postgresPassword))
    await kubectl([
      'create',
      'configmap',
      'reel-ca',
      '--namespace',
      NAMESPACE,
      `--from-file=ca.crt=${join(tls, INSTALL_TLS_FILES.ca)}`,
    ])
    // The work folder is private to this run and goes when it ends.
    const secretFile = join(work, 'coral.env')
    await writeFile(secretFile, secretEnv(secrets, postgresPassword), { mode: 0o600 })
    await kubectl([
      'create',
      'secret',
      'generic',
      'coral',
      '--namespace',
      NAMESPACE,
      `--from-env-file=${secretFile}`,
    ])
    await kubectl([
      'rollout',
      'status',
      'deployment/postgres',
      '--namespace',
      NAMESPACE,
      '--timeout',
      '180s',
    ])
    console.log('Reel install: installing the chart')
    const values = join(work, 'values.json')
    await writeFile(values, JSON.stringify(chartValues(secrets, options)))
    await exec('helm', [
      'install',
      'coral',
      join(repoRoot, 'charts/coral'),
      '--namespace',
      NAMESPACE,
      '--values',
      values,
      '--wait',
      '--timeout',
      '15m',
    ])
    console.log(`Reel install: ready at https://${INSTALL_HOSTS.web}, through ${address}`)
    // In a file, not on the command line, where `ps` would show the secret.
    const containerEnv = join(work, 'container.env')
    await writeFile(
      containerEnv,
      `E2E_INSTALL_OWNER_SUBJECT=${secrets.ownerSubject}\nE2E_INSTALL_CLIENT_SECRET=${secrets.clientSecret}\n`,
      { mode: 0o600 },
    )
    return {
      containerArgs: [
        '--network',
        'kind',
        // The identity provider and the provider fixtures run in the container.
        '--network-alias',
        INSTALL_HOSTS.idp,
        ...[INSTALL_HOSTS.web, INSTALL_HOSTS.auth, INSTALL_HOSTS.mcp].flatMap((host) => [
          '--add-host',
          `${host}:${address}`,
        ]),
        '--mount',
        `type=bind,src=${tls},dst=${INSTALL_TLS_DIRECTORY},readonly`,
        '--env-file',
        containerEnv,
      ],
      stop,
    }
  } catch (error) {
    try {
      await stop()
    } catch (cleanup) {
      console.warn(cleanup)
    }
    throw error
  }
}

/** A throwaway CA, and the Gateway's, the identity provider's and Postgres's certificates. */
async function certificates(directory: string) {
  await mkdir(directory, { recursive: true, mode: 0o700 })
  const path = (name: string) => join(directory, name)
  await run('openssl', [
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-days',
    '2',
    '-subj',
    '/CN=Reel install CA',
    '-keyout',
    path('ca.key'),
    '-out',
    path(INSTALL_TLS_FILES.ca),
  ])
  const leaf = async (name: string, hosts: string[]) => {
    await run('openssl', [
      'req',
      '-new',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-subj',
      `/CN=${hosts[0]}`,
      '-keyout',
      path(`${name}.key`),
      '-out',
      path(`${name}.csr`),
    ])
    await writeFile(
      path(`${name}.ext`),
      `subjectAltName=${hosts.map((host) => `DNS:${host}`).join(',')}\nextendedKeyUsage=serverAuth\n`,
    )
    await run('openssl', [
      'x509',
      '-req',
      '-days',
      '1',
      '-in',
      path(`${name}.csr`),
      '-CA',
      path(INSTALL_TLS_FILES.ca),
      '-CAkey',
      path('ca.key'),
      '-CAcreateserial',
      '-extfile',
      path(`${name}.ext`),
      '-out',
      path(`${name}.pem`),
    ])
  }
  await leaf('gateway', [INSTALL_HOSTS.web, INSTALL_HOSTS.auth, INSTALL_HOSTS.mcp])
  await leaf('idp', [INSTALL_HOSTS.idp])
  await leaf('postgres', [POSTGRES_HOST])
  // The container's root reads these, whoever owns them on the host.
  await run('chmod', ['0644', ...Object.values(INSTALL_TLS_FILES).map(path)])
  const secrets: InstallSecrets = {
    ownerSubject: `reel-${randomUUID()}`,
    clientSecret: randomBytes(32).toString('hex'),
  }
  return secrets
}

function gatewayManifest() {
  return `
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: eg
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: eg
  namespace: ${NAMESPACE}
spec:
  gatewayClassName: eg
  listeners:
    - name: https
      protocol: HTTPS
      port: 443
      tls:
        mode: Terminate
        certificateRefs:
          - name: gateway-tls
      allowedRoutes:
        namespaces:
          from: Same
`
}

async function gatewayAddress(kubectl: (args: string[]) => Promise<string>) {
  await kubectl([
    'wait',
    'gateway/eg',
    '--namespace',
    NAMESPACE,
    '--for=condition=Programmed',
    '--timeout=180s',
  ])
  const address = (
    await kubectl([
      'get',
      'gateway/eg',
      '--namespace',
      NAMESPACE,
      '--output',
      'jsonpath={.status.addresses[0].value}',
    ])
  ).trim()
  if (!/^\d+\.\d+\.\d+\.\d+$/.test(address))
    throw new Error(`The Gateway has no IPv4 address on kind's network: ${address || 'none'}`)
  return address
}

/** Adds the Install's hosts to CoreDNS, pointing at the Gateway. */
async function pointHostsAt(
  kubectl: (args: string[], input?: string) => Promise<string>,
  address: string,
) {
  const corefile = await kubectl([
    'get',
    'configmap',
    'coredns',
    '--namespace',
    'kube-system',
    '--output',
    'jsonpath={.data.Corefile}',
  ])
  const hosts = `    hosts {\n        ${address} ${INSTALL_HOSTS.web} ${INSTALL_HOSTS.auth} ${INSTALL_HOSTS.mcp}\n        fallthrough\n    }\n`
  const at = corefile.indexOf('    forward ')
  if (at < 0) throw new Error('CoreDNS has no forward block to put the hosts before')
  await kubectl([
    'patch',
    'configmap',
    'coredns',
    '--namespace',
    'kube-system',
    '--type',
    'merge',
    '--patch',
    JSON.stringify({ data: { Corefile: corefile.slice(0, at) + hosts + corefile.slice(at) } }),
  ])
  await kubectl(['rollout', 'restart', 'deployment/coredns', '--namespace', 'kube-system'])
  await kubectl([
    'rollout',
    'status',
    'deployment/coredns',
    '--namespace',
    'kube-system',
    '--timeout',
    '120s',
  ])
}

/** A test-only Postgres with pgvector, serving TLS that `sslmode=verify-full` checks. */
function postgresManifest(password: string) {
  return `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels: {app: postgres}
  template:
    metadata:
      labels: {app: postgres}
    spec:
      securityContext:
        # Postgres reads a key that root owns only when its group, and no one
        # else, can read it.
        fsGroup: 999
      containers:
        - name: postgres
          image: ${POSTGRES_IMAGE}
          args:
            - -c
            - ssl=on
            - -c
            - ssl_cert_file=/tls/tls.crt
            - -c
            - ssl_key_file=/tls/tls.key
          env:
            - name: POSTGRES_PASSWORD
              value: ${JSON.stringify(password)}
          ports:
            - containerPort: 5432
          readinessProbe:
            exec:
              command: [pg_isready, -U, postgres]
            periodSeconds: 2
          volumeMounts:
            - name: tls
              mountPath: /tls
      volumes:
        - name: tls
          secret:
            secretName: postgres-tls
            defaultMode: 0640
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: ${NAMESPACE}
spec:
  selector: {app: postgres}
  ports:
    - port: 5432
`
}

/** The chart's Secret, as an env file. */
function secretEnv(secrets: InstallSecrets, postgresPassword: string) {
  const { privateKey } = generateKeyPairSync('ec', {
    namedCurve: 'prime256v1',
    privateKeyEncoding: { format: 'der', type: 'pkcs8' },
    publicKeyEncoding: { format: 'pem', type: 'spki' },
  })
  return Object.entries({
    CORAL_DATABASE_URL: `postgres://postgres:${postgresPassword}@${POSTGRES_HOST}:5432/postgres?sslmode=verify-full`,
    CORAL_CREDENTIALS_KEY: randomBytes(32).toString('base64'),
    CORAL_SESSION_SIGNING_KEY: privateKey.toString('base64'),
    CORAL_OIDC_CLIENT_SECRET: secrets.clientSecret,
    CORAL_WEB_SESSION_SECRET: randomBytes(32).toString('hex'),
    CORAL_RUN_EXECUTOR_TOKEN: randomBytes(32).toString('hex'),
  })
    .map(([key, value]) => `${key}=${value}`)
    .join('\n')
}

/** An image the cluster already holds, never pulled. */
function localImage(reference: string) {
  // The tag follows the last colon after the last slash: a registry may have a port.
  const colon = reference.lastIndexOf(':')
  if (colon <= reference.lastIndexOf('/')) throw new Error(`Name a tag for ${reference}`)
  return {
    registry: '',
    repository: reference.slice(0, colon),
    tag: reference.slice(colon + 1),
    pullPolicy: 'Never',
  }
}

/** Small, so the suite fits a laptop's cluster. */
function memory(size: string) {
  return { requests: { cpu: '100m', memory: size }, limits: { memory: size } }
}

function chartValues(secrets: InstallSecrets, options: InstallOptions) {
  const web = `https://${INSTALL_HOSTS.web}`
  const idp = `https://${INSTALL_HOSTS.idp}`
  return {
    host: INSTALL_HOSTS.web,
    coral: { image: localImage(options.coralImage), replicas: 1, resources: memory('1Gi') },
    web: { image: localImage(options.webImage), replicas: 1, resources: memory('512Mi') },
    runExecutors: { replicas: 1, resources: memory('1Gi') },
    httpRoute: { enabled: true, parentRefs: [{ name: 'eg' }] },
    // One CA signs the Gateway's, the identity provider's and Postgres's
    // certificates, so this run proves both CA values.
    tls: { extraCABundle: { configMapName: 'reel-ca', key: 'ca.crt' } },
    database: { tls: { caBundle: { configMapName: 'reel-ca', key: 'ca.crt' } } },
    usageReports: { enabled: false },
    config: {
      auth: {
        // The identity provider also hosts a second OAuth client, which
        // Reel's actors use for management and MCP tokens.
        allowed_audiences: [web, idp],
        trusted_clients: [`${web}/.well-known/oauth-client`, `${idp}/.well-known/oauth-client`],
        provider: {
          issuer: idp,
          client_id: FIXTURE_CLIENT_ID,
          bootstrap_owner_subject: secrets.ownerSubject,
        },
      },
      // The identity provider and the provider fixtures run in the Reel
      // container, at a private address on kind's network.
      outbound: { private_networks: 'http-and-https' },
      search: { semantic_search_enabled: false },
    },
  }
}

/** What a failed run needs explained: every object, every event and every pod's log. */
async function collectEvidence(kubectl: (args: string[]) => Promise<string>, directory: string) {
  await mkdir(directory, { recursive: true })
  const save = async (file: string, args: string[]) => {
    try {
      await writeFile(join(directory, file), await kubectl(args))
    } catch (error) {
      await writeFile(join(directory, file), `kubectl ${args.join(' ')} failed: ${String(error)}`)
    }
  }
  await save('objects.txt', [
    'get',
    'all,gateway,httproute',
    '--all-namespaces',
    '--output',
    'wide',
  ])
  await save('events.txt', ['get', 'events', '--all-namespaces', '--sort-by=.lastTimestamp'])
  // Every pod's: Envoy Gateway's and CoreDNS's explain a routing failure.
  const pods = await kubectl([
    'get',
    'pods',
    '--all-namespaces',
    '--output',
    'jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name}{"\\n"}{end}',
  ]).catch(() => '')
  for (const pod of pods.split('\n').filter(Boolean)) {
    const [namespace, name] = pod.split('/')
    await save(`${namespace}.${name}.log`, [
      'logs',
      name,
      '--namespace',
      namespace,
      '--all-containers',
      '--timestamps',
    ])
  }
}

/** Fails before anything starts, naming what to install or build. */
async function checkPrerequisites(options: InstallOptions) {
  for (const [tool, args] of [
    ['kind', ['version']],
    ['kubectl', ['version', '--client']],
    ['helm', ['version']],
    ['openssl', ['version']],
  ] as const) {
    try {
      await run(tool, [...args])
    } catch {
      throw new Error(
        `The helm-install suite needs ${tool} on PATH; see "The Helm chart on kind" in apps/reef/e2e/README.md`,
      )
    }
  }
  for (const image of [options.coralImage, options.webImage]) {
    try {
      await run('docker', ['image', 'inspect', image])
    } catch {
      throw new Error(`No image ${image}; build it with make docker-build reef-docker-build`)
    }
  }
}

/** Runs `command` with `input` on its stdin. */
function pipe(
  command: string,
  args: string[],
  input: string,
  env: NodeJS.ProcessEnv,
  signal?: AbortSignal,
) {
  return new Promise<string>((resolve, reject) => {
    const child = spawn(command, args, { env, signal, stdio: ['pipe', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => (stdout += chunk))
    child.stderr.on('data', (chunk) => (stderr += chunk))
    child.once('error', reject)
    child.once('exit', (code) =>
      code === 0
        ? resolve(stdout)
        : reject(new Error(`${command} failed (${code}): ${stderr.trim()}`)),
    )
    child.stdin.end(input)
  })
}
