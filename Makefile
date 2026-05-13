# delayq Makefile

GO ?= go
GOFLAGS ?=

.PHONY: all build test test-race cover lint vet tidy proto clean help

all: lint test ## 默认：lint + test

build: ## 编译
	$(GO) build $(GOFLAGS) ./...

test: ## 运行测试
	$(GO) test $(GOFLAGS) -count=1 -timeout 240s ./...

test-race: ## 运行测试（含 race detector）
	$(GO) test $(GOFLAGS) -race -count=1 -timeout 240s ./...

test-integration: ## 运行 Redis 集成测试（需要 REDIS_ADDR 或本地 docker）
	@if [ -z "$(REDIS_ADDR)" ]; then \
		echo "starting docker redis..."; \
		docker run -d --rm -p 6379:6379 --name delayq-redis-test redis:7-alpine >/dev/null; \
		sleep 2; \
		REDIS_ADDR=127.0.0.1:6379 $(GO) test -tags=integration -race -count=1 -timeout 5m -run TestIntegration ./...; \
		ec=$$?; \
		docker stop delayq-redis-test >/dev/null; \
		exit $$ec; \
	else \
		$(GO) test -tags=integration -race -count=1 -timeout 5m -run TestIntegration ./...; \
	fi

cover: ## 生成覆盖率报告
	$(GO) test $(GOFLAGS) -race -count=1 -coverprofile=coverage.out -timeout 240s ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html in browser"

vet: ## go vet
	$(GO) vet ./...

lint: vet ## 静态检查（vet + staticcheck + errcheck，若已安装）
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed, skip"
	@command -v errcheck >/dev/null 2>&1 && errcheck ./... || echo "errcheck not installed, skip"
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed, skip"

tidy: ## 整理 go.mod
	$(GO) mod tidy

proto: ## 重新生成 item.pb.go（需要 protoc + protoc-gen-go）
	@command -v protoc >/dev/null 2>&1 || (echo "protoc not installed" && exit 1)
	@command -v protoc-gen-go >/dev/null 2>&1 || (echo "run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	protoc --go_out=paths=source_relative:. item.proto
	@echo "注意：原 item.pb.go 由 sandwich-go protokit 生成，含 db: tag。直接 protoc 生成不会包含该 tag。"

clean: ## 清理构建产物
	rm -f coverage.out coverage.html

help: ## 显示帮助
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
