FROM golang:1.26-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /cafi-server ./cmd/cafi-server/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /cafi-server /usr/local/bin/cafi-server
ENTRYPOINT ["cafi-server"]
CMD ["serve"]
