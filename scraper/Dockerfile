# Multi-stage build for Go Scraper
FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/scraper main.go

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/scraper /app/scraper

RUN mkdir -p /app/logs && chown -R nobody:nobody /app

USER nobody:nobody

ENTRYPOINT ["/app/scraper"]
