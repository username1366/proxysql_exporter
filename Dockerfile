FROM golang:1.20 as builder

ARG upx_version=4.0.0
ARG GOPROXY

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends xz-utils && \
  curl -Ls https://github.com/upx/upx/releases/download/v${upx_version}/upx-${upx_version}-amd64_linux.tar.xz -o - | tar xvJf - -C /tmp && \
  cp /tmp/upx-${upx_version}-amd64_linux/upx /usr/local/bin/ && \
  chmod +x /usr/local/bin/upx && \
  apt-get remove -y xz-utils && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOPROXY=https://proxy.golang.org go build -o app -ldflags="-s -w" main.go && \
    strip app && \
    /usr/local/bin/upx --best --lzma app

FROM alpine:latest as proxysql-exporter
WORKDIR /app
COPY --from=builder /app/app .