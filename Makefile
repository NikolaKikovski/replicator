.PHONY: all build test clean lint run

APP_NAME=replicator
MAIN_PATH=cmd/replicator/main.go

all: build

build:
	go build -o bin/$(APP_NAME) $(MAIN_PATH)

run:
	go run $(MAIN_PATH)

test:
	go test -v -race ./...

test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

e2e-test:
	./scripts/e2e_test.sh

stress-test:
	./scripts/stress_test.sh 100000 1000


chaos-test:
	./scripts/chaos_test.sh

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ coverage.out coverage.html
