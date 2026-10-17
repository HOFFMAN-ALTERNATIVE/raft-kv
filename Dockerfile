# syntax=docker/dockerfile:1
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/raftkv ./cmd/raftkv \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/raftkv /usr/local/bin/raftkv
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
EXPOSE 7001
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/raftkv", "-data", "/data"]
