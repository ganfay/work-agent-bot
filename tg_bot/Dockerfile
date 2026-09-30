# Multi-stage build for Go Telegram Bot
FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/tg_bot main.go

# Final minimal runtime image
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/tg_bot /app/tg_bot

RUN mkdir -p /app/logs && chown -R nobody:nobody /app

USER nobody:nobody

ENTRYPOINT ["/app/tg_bot"]
