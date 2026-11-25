{{/*
The name of every object, and the prefix of each component's: the release
name. Helm keeps it to 53 characters, so each suffix still fits a Service name.
*/}}
{{- define "coral.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "coral.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: coral
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels of one component. Call with (list $ "component"). */}}
{{- define "coral.selectorLabels" -}}
{{- $root := index . 0 -}}
app.kubernetes.io/name: coral
app.kubernetes.io/instance: {{ $root.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{/* Host names. Every public URL derives from them, always https. */}}
{{- define "coral.host.auth" -}}
{{- .Values.hosts.auth | default (printf "auth.%s" .Values.host) -}}
{{- end -}}

{{- define "coral.host.mcp" -}}
{{- .Values.hosts.mcp | default (printf "mcp.%s" .Values.host) -}}
{{- end -}}

{{/* Ports. Coral's listeners bind them; the Services expose them. */}}
{{- define "coral.port.grpc" -}}14555{{- end -}}
{{- define "coral.port.mcp" -}}14556{{- end -}}
{{- define "coral.port.auth" -}}8779{{- end -}}
{{- define "coral.port.runExecutor" -}}14557{{- end -}}
{{- define "coral.port.runExecutorHealth" -}}14558{{- end -}}
{{- define "coral.port.web" -}}3000{{- end -}}

{{/*
An image reference. Call with (list $ .Values.coral.image).
global.imageRegistry replaces the registry host and keeps the repository path.
*/}}
{{- define "coral.image" -}}
{{- $root := index . 0 -}}
{{- $image := index . 1 -}}
{{- $registry := $root.Values.global.imageRegistry | default $image.registry -}}
{{- $name := $image.repository -}}
{{- if $registry -}}
{{- $name = printf "%s/%s" $registry $image.repository -}}
{{- end -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $name $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $name ($image.tag | default $root.Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{/*
An env var read from a Secret. Call with (list $ "ENV_NAME" "values.path" $ref).
$ref is a {secretName, key}; an empty secretName means the release name, and
an empty key fails the render.
*/}}
{{- define "coral.secretEnv" -}}
{{- $root := index . 0 -}}
{{- $ref := index . 3 -}}
{{- $key := required (printf "%s.key is required: name the key of the Secret that holds it" (index . 2)) $ref.key -}}
- name: {{ index . 1 }}
  valueFrom:
    secretKeyRef:
      name: {{ $ref.secretName | default (include "coral.fullname" $root) }}
      key: {{ $key }}
{{- end -}}

{{/*
The config.toml keys the chart writes. Each is chart-owned: the render fails
when `config` sets one.
*/}}
{{- define "coral.ownedConfig" -}}
listeners:
  grpc:
    bind: 0.0.0.0:{{ include "coral.port.grpc" . }}
  mcp:
    bind: 0.0.0.0:{{ include "coral.port.mcp" . }}
    public_url: https://{{ include "coral.host.mcp" . }}
  auth:
    bind: 0.0.0.0:{{ include "coral.port.auth" . }}
    public_url: https://{{ include "coral.host.auth" . }}
  web:
    public_url: https://{{ .Values.host }}
  run_executor:
    bind: 0.0.0.0:{{ include "coral.port.runExecutor" . }}
    token_env:
      - CORAL_RUN_EXECUTOR_TOKEN
      {{- if .Values.runExecutors.previousToken.key }}
      - CORAL_RUN_EXECUTOR_PREVIOUS_TOKEN
      {{- end }}
runtime:
  local_run_executors: false
database:
  backend: postgres
  url_env: CORAL_DATABASE_URL
credentials:
  key_env: CORAL_CREDENTIALS_KEY
auth:
  session:
    signing_key_env: CORAL_SESSION_SIGNING_KEY
  provider:
    client_secret_env: CORAL_OIDC_CLIENT_SECRET
{{- if .Values.secrets.license.enabled }}
license:
  key_env: CORAL_LICENSE
{{- end }}
{{- end -}}

{{/*
Every chart-owned path in config.toml. It also lists the keys that would
take the place of one the chart sets, such as `auth.provider.client_secret`.
*/}}
{{- define "coral.ownedPaths" -}}
- listeners.grpc
- listeners.mcp
- listeners.auth
- listeners.web
- listeners.run_executor
- runtime.local_run_executors
- database
- credentials.key_env
- auth.session.signing_key_env
- auth.session.signing_key_file
- auth.provider.client_secret_env
- auth.provider.client_secret
- license
{{- end -}}

{{/* Fails the render when `config` sets a chart-owned path. */}}
{{- define "coral.checkConfig" -}}
{{- range $path := include "coral.ownedPaths" . | fromYamlArray -}}
{{- $node := $.Values.config -}}
{{- $state := "walking" -}}
{{- range $segment := splitList "." $path -}}
{{- if eq $state "walking" -}}
{{- if not (kindIs "map" $node) -}}
{{- $state = "hit" -}}
{{- else if hasKey $node $segment -}}
{{- $node = index $node $segment -}}
{{- else -}}
{{- $state = "miss" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if ne $state "miss" -}}
{{- fail (printf "config.%s is set by the chart: remove it from config, and use the chart's values instead" $path) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Turns whole numbers back into integers, in place. Helm reads every number in
values as a float, and toToml would write 134217728 as 1.34217728e+08.
*/}}
{{- define "coral.integers" -}}
{{- $map := . -}}
{{- range $key, $value := $map -}}
{{- if and (kindIs "float64" $value) (eq $value (floor $value)) -}}
{{- $_ := set $map $key (int64 $value) -}}
{{- else if kindIs "map" $value -}}
{{- include "coral.integers" $value -}}
{{- else if kindIs "slice" $value -}}
{{- $items := list -}}
{{- range $item := $value -}}
{{- if and (kindIs "float64" $item) (eq $item (floor $item)) -}}
{{- $items = append $items (int64 $item) -}}
{{- else -}}
{{- if kindIs "map" $item -}}
{{- include "coral.integers" $item -}}
{{- end -}}
{{- $items = append $items $item -}}
{{- end -}}
{{- end -}}
{{- $_ := set $map $key $items -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* The whole config.toml. */}}
{{- define "coral.configToml" -}}
{{- include "coral.checkConfig" . -}}
{{- $user := deepCopy .Values.config -}}
{{- include "coral.integers" $user -}}
{{- $config := mustMergeOverwrite $user (include "coral.ownedConfig" . | fromYaml) -}}
{{- toToml $config -}}
{{- end -}}

{{/* Probe timings. Call with a probe's values. */}}
{{- define "coral.probeTimings" -}}
periodSeconds: {{ .periodSeconds }}
timeoutSeconds: {{ .timeoutSeconds }}
failureThreshold: {{ .failureThreshold }}
{{- end -}}

{{/* The container security context of every pod the chart runs. */}}
{{- define "coral.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end -}}
