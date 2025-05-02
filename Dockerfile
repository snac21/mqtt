# Build stage
FROM golang:1.19-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git make protoc

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN make build

# Final stage
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /app/bin/mqtt-broker .

# Expose ports
EXPOSE 1883 8080

# Run the application
CMD ["./mqtt-broker"] 