{{/*
Expand the name of the chart.
*/}}
{{- define "knowledge-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "knowledge-platform.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "knowledge-platform.labels" -}}
helm.sh/chart: {{ include "knowledge-platform.chart" . }}
{{ include "knowledge-platform.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Chart label
*/}}
{{- define "knowledge-platform.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "knowledge-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "knowledge-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Component-specific names
*/}}
{{- define "knowledge-platform.ingestionWorker.fullname" -}}
{{- printf "%s-ingestion-worker" (include "knowledge-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "knowledge-platform.mcpServer.fullname" -}}
{{- printf "%s-mcp-server" (include "knowledge-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Service account name (shared)
*/}}
{{- define "knowledge-platform.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "knowledge-platform.fullname" .) .Values.serviceAccount.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
