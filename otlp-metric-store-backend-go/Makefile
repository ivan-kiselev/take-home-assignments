MODULE := dash0.com/otlp-log-processor-backend

.PHONY: build run test test-integration test-all loadtest fmt vet lint tidy clean

build:
	go build ./...

run:
	go run .

test:
	go test -count=1 ./...

test-integration:
	go test -tags integration -count=1 -v ./...

test-all: test test-integration

# Load test against a throwaway ClickHouse (testcontainers). Writes artifacts to bench/.
# Tune via env: LOAD_SERVICES, LOAD_METRICS, LOAD_ATTR_COMBOS, LOAD_DATAPOINTS, LOAD_DP_PER_REQ, LOAD_WORKERS.
# Override LOAD_LABEL / LOAD_SCHEMA to name the artifact (e.g. baseline vs after).
loadtest:
	go test -tags loadtest -count=1 -v -timeout 30m -run TestLoadBaseline .

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: vet
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

tidy:
	go mod tidy

clean:
	go clean ./...
