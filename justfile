# kstreams - Task Runner
#
# Just is a command runner (https://github.com/casey/just)
# Usage: just <task>
# List all tasks: just --list

# Default recipe runs help
default:
    @just --list

# ==============================================================================
# Setup & Installation
# ==============================================================================

# Install all development dependencies
install-deps:
    @echo "Installing Go dependencies..."
    go mod download
    @echo "Installing golangci-lint..."
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin
    @echo "✅ All dependencies installed!"

# ==============================================================================
# Building
# ==============================================================================

# Build all packages
build:
    @echo "Building all packages..."
    go build -v ./...
    @echo "✅ Build complete!"

# Build with race detector
build-race:
    @echo "Building with race detector..."
    go build -race -v ./...
    @echo "✅ Race build complete!"

# Clean build artifacts
clean:
    @echo "Cleaning build artifacts..."
    go clean -cache -testcache
    rm -rf coverage.txt coverage.html *.test
    @echo "✅ Clean complete!"

# ==============================================================================
# Testing
# ==============================================================================

# Run all tests
test:
    @echo "Running all tests..."
    go test -v -race ./...
    @echo "✅ All tests passed!"

# Run tests with coverage
test-coverage:
    @echo "Running tests with coverage..."
    go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
    go tool cover -html=coverage.txt -o coverage.html
    @echo "✅ Coverage report generated: coverage.html"

# Run only unit tests (short mode, cacheable)
test-unit:
    @echo "Running unit tests (short mode, cacheable)..."
    go test -short ./...
    @echo "✅ Unit tests passed!"

# Run unit tests with race detector
test-unit-race:
    @echo "Running unit tests with race detector..."
    go test -short -race ./...
    @echo "✅ Unit tests passed!"

# Run integration tests (requires Kafka)
test-integration:
    @echo "Running integration tests..."
    go test -v ./integrationtest/...
    @echo "✅ Integration tests passed!"

# Run tests matching a pattern
test-match pattern:
    @echo "Running tests matching: {{pattern}}"
    go test -v -race -run {{pattern}} ./...

# ==============================================================================
# Linting & Formatting
# ==============================================================================

# Run golangci-lint
lint:
    @echo "Running golangci-lint..."
    golangci-lint run ./...
    @echo "✅ Lint complete!"

# Run golangci-lint and auto-fix issues
lint-fix:
    @echo "Running golangci-lint with auto-fix..."
    golangci-lint run --fix ./...
    @echo "✅ Lint fix complete!"

# Format Go code
fmt:
    @echo "Formatting Go code..."
    go fmt ./...
    @echo "✅ Format complete!"

# Run goimports
imports:
    @echo "Running goimports..."
    goimports -w .
    @echo "✅ Imports organized!"

# Run all formatters
format: fmt imports
    @echo "✅ All formatting complete!"

# ==============================================================================
# Code Quality
# ==============================================================================

# Run static analysis
vet:
    @echo "Running go vet..."
    go vet ./...
    @echo "✅ Vet complete!"

# Run all quality checks
quality: vet lint
    @echo "✅ All quality checks passed!"

# ==============================================================================
# CI/CD Simulation
# ==============================================================================

# Run all CI checks locally
ci: quality test-unit build
    @echo "✅ All CI checks passed!"

# Run full CI pipeline (including integration)
ci-full: quality test build
    @echo "✅ Full CI pipeline passed!"

# Pre-commit checks (fast)
pre-commit: fmt lint-fix test-unit
    @echo "✅ Pre-commit checks passed!"

# ==============================================================================
# Development Helpers
# ==============================================================================

# Run Go mod tidy
tidy:
    @echo "Running go mod tidy..."
    go mod tidy
    @echo "✅ Go mod tidy complete!"

# Update all dependencies
update-deps:
    @echo "Updating dependencies..."
    go get -u ./...
    go mod tidy
    @echo "✅ Dependencies updated!"

# Verify dependencies
verify:
    @echo "Verifying dependencies..."
    go mod verify
    @echo "✅ Dependencies verified!"

# ==============================================================================
# Benchmarking
# ==============================================================================

# Run benchmarks
bench:
    @echo "Running benchmarks..."
    go test -bench=. -benchmem ./...

# Run benchmarks with CPU profiling
bench-cpu:
    @echo "Running benchmarks with CPU profiling..."
    go test -bench=. -benchmem -cpuprofile=cpu.prof ./...
    @echo "View profile with: go tool pprof cpu.prof"

# ==============================================================================
# Troubleshooting
# ==============================================================================

# Show environment info
env-info:
    @echo "Environment Information:"
    @echo "======================="
    @echo "Go version: $(go version)"
    @echo "GOPATH: $(go env GOPATH)"
    @echo "GOOS: $(go env GOOS)"
    @echo "GOARCH: $(go env GOARCH)"
    @which golangci-lint > /dev/null && echo "golangci-lint: $(golangci-lint --version)" || echo "golangci-lint: not installed"

# Check if all tools are installed
check-tools:
    @echo "Checking required tools..."
    @which go > /dev/null && echo "✅ Go" || echo "❌ Go"
    @which golangci-lint > /dev/null && echo "✅ golangci-lint" || echo "❌ golangci-lint"
    @echo ""
    @echo "Install missing tools with: just install-deps"
