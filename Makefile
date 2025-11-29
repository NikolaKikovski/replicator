.PHONY: all build test clean lint run

APP_NAME=replicator
MAIN_PATH=cmd/replicator/main.go

all: build

build:
	go build -o bin/$(APP_NAME) $(MAIN_PATH)

run:
	go run $(MAIN_PATH)

test:
	go test -v ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

e2e-test:
	./scripts/e2e_test.sh

stress-test:
	./scripts/stress_test.sh 10000 1000


chaos-test:
	./scripts/chaos_test.sh

test-all:
	@echo "=========================================="
	@echo "Running Complete Test Suite"
	@echo "=========================================="
	@echo ""
	@echo "Building replicator binary..."
	@$(MAKE) build
	@echo ""
	@echo "Starting docker-compose..."
	@docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo ""
	@echo "1/4: Unit Tests"
	@echo "------------------------------------------"
	@$(MAKE) test
	@echo ""
	@echo "2/4: End-to-End Test"
	@echo "------------------------------------------"
	@$(MAKE) e2e-test
	@echo ""
	@echo "3/4: Stress Test (10k rows)"
	@echo "------------------------------------------"
	@$(MAKE) stress-test
	@echo ""
	@echo "4/4: Chaos Test (Crash Recovery)"
	@echo "------------------------------------------"
	@$(MAKE) chaos-test
	@echo ""
	@echo "Stopping replicator..."
	@pkill -f bin/replicator || true
	@echo "Stopping docker-compose..."
	@docker-compose down
	@echo ""
	@echo "Cleaning up..."
	@$(MAKE) clean
	@echo ""
	@echo "=========================================="
	@echo "✅ ALL TESTS PASSED!"
	@echo "=========================================="

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ coverage.out coverage.html
