# Build stage
FROM golang:1.26-alpine AS builder

# Build arguments for version injection
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG GIT_TREE_STATE=unknown
ARG BUILD_DATE=unknown

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.mod
COPY go.sum go.sum

# Cache dependencies
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY internal/ internal/

# Build the binary with Search-specific version information
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-X 'go.miloapis.net/search/internal/version.Version=${VERSION}' \
              -X 'go.miloapis.net/search/internal/version.GitCommit=${GIT_COMMIT}' \
              -X 'go.miloapis.net/search/internal/version.GitTreeState=${GIT_TREE_STATE}' \
              -X 'go.miloapis.net/search/internal/version.BuildDate=${BUILD_DATE}'" \
    -a -o search ./cmd/search

# Runtime stage
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/search .
USER 65532:65532

ENTRYPOINT ["/search"]
