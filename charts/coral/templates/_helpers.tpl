{{/*
The name of every object, and the prefix of each component's: the release
name. Helm keeps it to 53 characters, so each suffix still fits a Service name.
Helm accepts a release name with a dot or a leading digit, which a Service
name may not have, so the render fails on one.
*/}}
{{- define "coral.fullname" -}}
{{- if not (regexMatch "^[a-z]([-a-z0-9]*[a-z0-9])?$" .Release.Name) -}}
{{- fail (printf "release name %q cannot name a Service: use lowercase letters, digits and hyphens, starting with a letter" .Release.Name) -}}
{{- end -}}
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

{{/*
What routing sends to each host: the host without a port, and the Service and
port behind it. coral's gRPC Service is never among them.
*/}}
{{- define "coral.routes" -}}
{{- $fullname := include "coral.fullname" . -}}
{{- $routes := list
  (list .Values.host "web" (include "coral.port.web" .))
  (list (include "coral.host.auth" .) "auth" (include "coral.port.auth" .))
  (list (include "coral.host.mcp" .) "mcp" (include "coral.port.mcp" .)) -}}
{{- $seen := dict -}}
{{- range $routes }}
{{- $host := regexReplaceAll ":[0-9]+$" (index . 0) "" }}
{{- if hasKey $seen $host }}
{{- fail (printf "host, hosts.auth and hosts.mcp must differ: %s is used twice" $host) }}
{{- end }}
{{- $_ := set $seen $host true }}
- host: {{ $host | quote }}
  service: {{ printf "%s-%s" $fullname (index . 1) | quote }}
  port: {{ index . 2 }}
{{- end -}}
{{- end -}}

{{/* Ports. Coral's listeners bind them; the Services expose them. */}}
{{- define "coral.port.grpc" -}}14555{{- end -}}
{{- define "coral.port.mcp" -}}14556{{- end -}}
{{- define "coral.port.auth" -}}8779{{- end -}}
{{- define "coral.port.runExecutor" -}}14557{{- end -}}
{{- define "coral.port.runExecutorHealth" -}}14558{{- end -}}
{{- define "coral.port.web" -}}3000{{- end -}}
{{/* coral-web's Envoy sidecar: gRPC on loopback, and its health check. */}}
{{- define "coral.port.envoy" -}}14559{{- end -}}
{{- define "coral.port.envoyHealth" -}}14560{{- end -}}

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
      name: {{ $ref.secretName | default (include "coral.fullname" $root) | quote }}
      key: {{ $key | quote }}
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

{{/*
A volume that holds a CA bundle as ca.crt, or nothing when the bundle has no
key. Call with (list "values.path" $bundle "volume-name").
*/}}
{{- define "coral.caBundleVolume" -}}
{{- $path := index . 0 -}}
{{- $bundle := index . 1 -}}
{{- include "coral.checkCABundle" (list $path $bundle) -}}
{{- if $bundle.key -}}
{{- if and $bundle.configMapName $bundle.secretName -}}
{{- fail (printf "%s: set configMapName or secretName, not both" $path) -}}
{{- end -}}
- name: {{ index . 2 }}
{{- if $bundle.configMapName }}
  configMap:
    name: {{ $bundle.configMapName | quote }}
{{- else if $bundle.secretName }}
  secret:
    secretName: {{ $bundle.secretName | quote }}
{{- else }}
{{- fail (printf "%s: set configMapName or secretName to name where the key is" $path) -}}
{{- end }}
    items:
      - key: {{ $bundle.key | quote }}
        path: ca.crt
{{- end -}}
{{- end -}}

{{/* Fails on a CA bundle that names a source but no key. Call with (list "values.path" $bundle). */}}
{{- define "coral.checkCABundle" -}}
{{- $bundle := index . 1 -}}
{{- if and (or $bundle.configMapName $bundle.secretName) (not $bundle.key) -}}
{{- fail (printf "%s: set key to name the PEM bundle in the ConfigMap or Secret" (index . 0)) -}}
{{- end -}}
{{- end -}}

{{/*
Fails when extraEnv sets a variable the chart sets: Kubernetes would keep the
last one, and silently replace the chart's. Call with
(list "values.path" $extraEnv (list "NAME" ...)).
*/}}
{{- define "coral.checkExtraEnv" -}}
{{- $path := index . 0 -}}
{{- $owned := index . 2 -}}
{{- range $env := index . 1 -}}
{{- if has $env.name $owned -}}
{{- fail (printf "%s sets %s, which the chart sets" $path $env.name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
coral-web's Envoy config. Envoy takes coral-web's gRPC on loopback and sends
each request to the next coral pod in turn. It finds the pods through the
headless Service, whose DNS name resolves to every ready pod.
*/}}
{{- define "coral.envoyConfig" -}}
# Every listener together. coral-web needs a few connections, and the
# kubelet's probe one. Without a cap, a flood of connections could take Envoy
# past its memory limit.
overload_manager:
  resource_monitors:
    - name: envoy.resource_monitors.global_downstream_max_connections
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.resource_monitors.downstream_connections.v3.DownstreamConnectionsConfig
        max_active_downstream_connections: 1000
static_resources:
  listeners:
    - name: coral
      address:
        socket_address:
          address: 127.0.0.1
          port_value: {{ include "coral.port.envoy" . }}
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: coral
                codec_type: HTTP2
                # A stream can stay quiet for as long as it waits, such as for
                # a Run's next event.
                stream_idle_timeout: 0s
                route_config:
                  virtual_hosts:
                    - name: coral
                      domains: ["*"]
                      routes:
                        - match:
                            prefix: /
                          route:
                            cluster: coral
                            timeout: 0s
                            # Only failures before coral sees the request, so
                            # a retry never runs a call twice.
                            retry_policy:
                              retry_on: connect-failure,refused-stream,reset-before-request
                              num_retries: 2
                http_filters:
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
    # For the kubelet's startup probe only. Any pod in the cluster can reach
    # it, so it has its own limit and drops idle connections: connections held
    # open here can't use up the cap above.
    - name: health
      address:
        socket_address:
          address: 0.0.0.0
          port_value: {{ include "coral.port.envoyHealth" . }}
      filter_chains:
        - filters:
            - name: envoy.filters.network.connection_limit
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.connection_limit.v3.ConnectionLimit
                stat_prefix: health
                max_connections: 100
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: health
                common_http_protocol_options:
                  idle_timeout: 10s
                route_config:
                  virtual_hosts:
                    - name: health
                      domains: ["*"]
                      routes:
                        - match:
                            prefix: /
                          direct_response:
                            status: 404
                http_filters:
                  - name: envoy.filters.http.health_check
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.health_check.v3.HealthCheck
                      pass_through_mode: false
                      headers:
                        - name: :path
                          string_match:
                            exact: /ready
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
    - name: coral
      # Each address the name resolves to is an endpoint.
      cluster_type:
        name: envoy.clusters.dns
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.clusters.dns.v3.DnsCluster
          dns_lookup_family: V4_PREFERRED
          # How soon a new coral pod takes requests, and a gone one stops.
          dns_refresh_rate: 5s
      lb_policy: ROUND_ROBIN
      # Every open stream counts, and each browser tab holds one: Envoy's
      # default of 1,024 would refuse requests long before coral is busy.
      circuit_breakers:
        thresholds:
          - max_connections: 100000
            max_pending_requests: 100000
            max_requests: 100000
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config:
            http2_protocol_options:
              # Finds a coral pod that went away without closing its
              # connections.
              connection_keepalive:
                interval: 30s
                timeout: 5s
      load_assignment:
        cluster_name: coral
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: {{ include "coral.fullname" . }}-headless.{{ .Release.Namespace }}.svc
                      port_value: {{ include "coral.port.grpc" . }}
{{- end -}}

{{/*
Where a component's pods run, and how they spread. Call with
(list $ .Values.component). The spread may name {{ .Release.Name }}.
*/}}
{{- define "coral.placement" -}}
{{- $root := index . 0 -}}
{{- $values := index . 1 -}}
{{- with $values.topologySpreadConstraints }}
topologySpreadConstraints:
  {{- tpl (toYaml .) $root | nindent 2 }}
{{- end }}
{{- with $values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $values.priorityClassName }}
priorityClassName: {{ . | quote }}
{{- end }}
{{- end -}}
