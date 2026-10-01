{{- define "kowiki.labels" -}}
app.kubernetes.io/name: kowiki
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{- define "kowiki.selector" -}}
app.kubernetes.io/name: kowiki
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "kowiki.image" -}}
image: {{ printf "%s:%s" .Values.image.repository .Values.image.tag | quote }}
imagePullPolicy: {{ .Values.image.pullPolicy }}
{{- end }}

{{- define "kowiki.env" -}}
envFrom:
  - configMapRef:
      name: {{ .Release.Name }}-config
  - secretRef:
      name: {{ .Release.Name }}-secret
{{- end }}

{{- define "kowiki.kafka" -}}
{{- .Values.kafka.bootstrap | default (printf "kowiki-kafka-bootstrap.%s.svc:9092" .Release.Namespace) -}}
{{- end }}

{{- define "kowiki.embeddingUrl" -}}
{{- if .Values.embedding.url -}}
{{ .Values.embedding.url }}
{{- else if eq .Values.embedding.kind "tei" -}}
http://{{ .Release.Name }}-embedding/v1
{{- else -}}
http://{{ .Release.Name }}-embedding:11434/v1
{{- end -}}
{{- end }}

{{- define "kowiki.s3Endpoint" -}}
{{- if .Values.storage.enabled -}}
http://{{ .Release.Name }}-s3:8333
{{- else -}}
{{ .Values.storage.endpoint }}
{{- end -}}
{{- end }}

{{- define "kowiki.checksum" -}}
checksum/config: {{ include (print .Template.BasePath "/config.yaml") . | sha256sum }}
{{- end }}
