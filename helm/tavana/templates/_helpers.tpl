{{/*
Expand the name of the chart.
*/}}
{{- define "tavana.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "tavana.fullname" -}}
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
{{- define "tavana.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "tavana.labels" -}}
helm.sh/chart: {{ include "tavana.chart" . }}
{{ include "tavana.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "tavana.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tavana.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "tavana.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tavana.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get the image registry
*/}}
{{- define "tavana.imageRegistry" -}}
{{- .Values.global.imageRegistry }}
{{- end }}

{{/*
Get the image tag
*/}}
{{- define "tavana.imageTag" -}}
{{- .Values.global.imageTag }}
{{- end }}

{{/*
Gateway image
*/}}
{{- define "tavana.gateway.image" -}}
{{- $registry := include "tavana.imageRegistry" . }}
{{- $repo := .Values.gateway.image.repository }}
{{- $tag := .Values.gateway.image.tag | default (include "tavana.imageTag" .) }}
{{- printf "%s/%s:%s" $registry $repo $tag }}
{{- end }}

{{/*
Operator image
*/}}
{{- define "tavana.operator.image" -}}
{{- $registry := include "tavana.imageRegistry" . }}
{{- $repo := .Values.operator.image.repository }}
{{- $tag := .Values.operator.image.tag | default (include "tavana.imageTag" .) }}
{{- printf "%s/%s:%s" $registry $repo $tag }}
{{- end }}

{{/*
Worker image
*/}}
{{- define "tavana.worker.image" -}}
{{- $registry := include "tavana.imageRegistry" . }}
{{- $repo := .Values.operator.worker.image.repository }}
{{- $tag := .Values.operator.worker.image.tag | default (include "tavana.imageTag" .) }}
{{- printf "%s/%s:%s" $registry $repo $tag }}
{{- end }}

{{/*
Catalog image
*/}}
{{- define "tavana.catalog.image" -}}
{{- $registry := include "tavana.imageRegistry" . }}
{{- $repo := .Values.catalog.image.repository }}
{{- $tag := .Values.catalog.image.tag | default (include "tavana.imageTag" .) }}
{{- printf "%s/%s:%s" $registry $repo $tag }}
{{- end }}

{{/*
Metering image
*/}}
{{- define "tavana.metering.image" -}}
{{- $registry := include "tavana.imageRegistry" . }}
{{- $repo := .Values.metering.image.repository }}
{{- $tag := .Values.metering.image.tag | default (include "tavana.imageTag" .) }}
{{- printf "%s/%s:%s" $registry $repo $tag }}
{{- end }}

{{/*
Database URL
*/}}
{{- define "tavana.databaseUrl" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "postgres://%s:%s@%s-postgresql:5432/%s" .Values.postgresql.auth.username .Values.postgresql.auth.password (include "tavana.fullname" .) .Values.postgresql.auth.database }}
{{- else }}
{{- printf "postgres://%s:%s@%s:%d/%s" .Values.externalDatabase.username .Values.externalDatabase.password .Values.externalDatabase.host (int .Values.externalDatabase.port) .Values.externalDatabase.database }}
{{- end }}
{{- end }}

