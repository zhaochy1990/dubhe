FROM golang:1.26-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/dubhe-tracing/ ./cmd/dubhe-tracing/
COPY internal/tracing/ ./internal/tracing/
COPY src/dubhe/schema.go src/dubhe/schema.sql ./src/dubhe/
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -mod=readonly -trimpath -o /out/dubhe-tracing ./cmd/dubhe-tracing

FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/dubhe-tracing /usr/local/bin/dubhe-tracing

ENV DUBHE_LISTEN=0.0.0.0:8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dubhe-tracing"]
CMD ["--config", "/etc/dubhe/config.yaml"]
