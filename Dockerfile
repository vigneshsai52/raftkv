FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o raftkv .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/raftkv .
EXPOSE 8080 12000
CMD ["./raftkv"]