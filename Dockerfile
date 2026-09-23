FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-X main.version=${VERSION}" -o rolloor ./cmd/rolloor && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -o rolloor-ethpandaops ./contrib/ethpandaops/cmd/rolloor-ethpandaops

FROM alpine:3.20

# The hook scripts an operator ships usually want these.
RUN apk add --no-cache ca-certificates tzdata bash curl jq && \
    adduser -D -u 10001 rolloor && \
    mkdir -p /etc/rolloor/targets.d /etc/rolloor/hooks /var/lib/rolloor && \
    chown -R rolloor /var/lib/rolloor

COPY --from=builder /app/rolloor /app/rolloor-ethpandaops /usr/local/bin/

# The ethpandaops hook programs, one link per program name.
RUN mkdir -p /usr/local/share/rolloor/ethpandaops && \
    for p in inspect update ready-running ready-beacon ready-execution soak-beacon soak-execution soak-validator environment; do \
      ln -s /usr/local/bin/rolloor-ethpandaops /usr/local/share/rolloor/ethpandaops/$p; \
    done

USER rolloor
EXPOSE 8080
VOLUME /var/lib/rolloor

ENTRYPOINT ["rolloor"]
CMD ["serve", "--config=/etc/rolloor/config.yaml"]
