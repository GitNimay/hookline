# One multi-stage build for every binary: docker build --build-arg SERVICE=worker .
FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG SERVICE
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/GitNimay/hookline/internal/telemetry.Version=${VERSION} -X github.com/GitNimay/hookline/internal/telemetry.Commit=${COMMIT} -X github.com/GitNimay/hookline/internal/telemetry.BuildDate=${BUILD_DATE}" \
      -o /out/app ./cmd/${SERVICE}

# distroless static, non-root (uid 65532), pinned by digest
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/app /app
USER 65532:65532
EXPOSE 8080 8081 9090
ENTRYPOINT ["/app"]
