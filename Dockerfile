FROM golang:1.23 as builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -o parser cmd/main.go

# Final stage
FROM debian:bookworm-slim

# Install required runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates tzdata chromium && \
    rm -rf /var/lib/apt/lists/*

# Create log directory with proper permissions
RUN mkdir -p /var/log && \
    touch /var/log/pinacle-parser.log && \
    chown -R 1000:1000 /var/log/pinacle-parser.log

# Copy the binary from builder
COPY --from=builder /app/parser /usr/local/bin/parser

# Copy config directory
COPY --from=builder /app/config /config

# Set environment variable for Chrome
ENV CHROMIUM_PATH=/usr/bin/chromium

# Run as non-root user
RUN useradd -r -u 1000 -m appuser
USER appuser

ENTRYPOINT ["parser"] 