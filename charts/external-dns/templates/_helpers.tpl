{{/*
Expand the name of the chart.
*/}}
{{- define "external-dns.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "external-dns.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "external-dns.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "external-dns.labels" -}}
helm.sh/chart: {{ include "external-dns.chart" . }}
{{ include "external-dns.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "external-dns.selectorLabels" -}}
app.kubernetes.io/name: {{ include "external-dns.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "external-dns.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "external-dns.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The image to use
*/}}
{{- define "external-dns.image" -}}
{{- printf "%s:%s" .Values.image.repository (default (printf "v%s" .Chart.AppVersion) .Values.image.tag) }}
{{- end }}

{{/*
Provider name, Keeps backward compatibility on provider
TODO: line eq (typeOf .Values.provider) "string" to be removed in future releases
*/}}
{{- define "external-dns.providerName" -}}
{{- if eq (typeOf .Values.provider) "string" }}
{{- .Values.provider }}
{{- else }}
{{- .Values.provider.name }}
{{- end }}
{{- end }}

{{/*
The image to use for optional webhook sidecar
*/}}
{{- define "external-dns.webhookImage" -}}
{{- with .image }}
{{- if or (empty .repository) (empty .tag) }}
{{- fail "ERROR: webhook provider needs an image repository and a tag" }}
{{- end }}
{{- printf "%s:%s" .repository .tag }}
{{- end }}
{{- end }}

{{/*
The pod affinity default label Selector
*/}}
{{- define "external-dns.labelSelector" -}}
labelSelector:
  matchLabels:
    {{ include "external-dns.selectorLabels" . | nindent 4 }}
{{- end }}

{{/*
Check if any Gateway API sources are enabled
*/}}
{{- define "external-dns.hasGatewaySources" -}}
{{- if or (has "gateway-httproute" .Values.sources) (has "gateway-grpcroute" .Values.sources) (has "gateway-tlsroute" .Values.sources) (has "gateway-tcproute" .Values.sources) (has "gateway-udproute" .Values.sources) -}}
true
{{- end -}}
{{- end }}
