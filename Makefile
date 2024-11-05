container_tag ?= rated-log-indexer-indexer
container_name ?= rated_log_indexer
redis_name ?= redis
network_name ?= rated_network

##@ 🚀  Getting started
.PHONY: run
run: clean start ## Ready, set, go!

.PHONY: clean
clean: ## Clean up existing containers and network
	@echo "Cleaning up existing containers..."
	@docker stop $(container_name) 2>/dev/null || true
	@docker stop $(redis_name) 2>/dev/null || true
	@docker network rm $(network_name) 2>/dev/null || true
	@echo "Cleanup complete"

.PHONY: start
start: ## Start everything fresh
	@docker network create $(network_name) 2>/dev/null || true
	@echo "Starting Redis..."
	@docker run -d --rm \
		--name $(redis_name) \
		--network $(network_name) \
		redis:alpine
	@sleep 2
	@echo "Building application..."
	@docker build -t $(container_tag) .
	@echo "Starting application..."
	@docker run -d --rm \
		--name $(container_name) \
		--network $(network_name) \
		$(container_tag)
	@echo "Containers started. Use 'make logs' to see output"

.PHONY: build
build:  ## Build the Docker container
	@docker build -t $(container_tag) .

.PHONY: up
up: ## Start services
	@docker run -d --rm --name $(container_name) $(container_tag)

.PHONY: down
down: clean

.PHONY: remove
remove: clean
	@docker rmi $(container_tag) || true

.PHONY: logs
logs: ## Output container logs.
	@docker logs --follow --tail 50 $(container_name)

.PHONY: ready
ready: ## Get ready to rumble
	@pre-commit install
	@pre-commit install --hook-type commit-msg

.PHONY: test
test:  ## Run tests in Docker, and optionally provide a path to a specific test file or directory
	@pytest $(path) -vv

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help
