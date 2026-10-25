{{/* Common labels for a component. Call with (dict "root" $ "name" $name) */}}
{{- define "hookline.labels" -}}
app.kubernetes.io/name: hookline
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
app.kubernetes.io/part-of: hookline
app.kubernetes.io/version: {{ .root.Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ .root.Chart.Name }}-{{ .root.Chart.Version }}
{{- end }}

{{- define "hookline.selector" -}}
app.kubernetes.io/name: hookline
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end }}

{{- define "hookline.image" -}}
{{ .root.Values.image.prefix }}{{ .name }}:{{ .root.Values.image.tag }}
{{- end }}

{{/* GOMEMLIMIT = 90% of the memory limit (limits are in Mi). */}}
{{- define "hookline.gomemlimit" -}}
{{- $mi := trimSuffix "Mi" .limits.memory | int -}}
{{ div (mul $mi 9) 10 }}MiB
{{- end }}

{{- define "hookline.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "hookline.containerSecurityContext" -}}
readOnlyRootFilesystem: true
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
{{- end }}
