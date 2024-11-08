container_tag ?= rated-log-indexer-indexer
container_name ?= rated_log_indexer
instance ?= 1
redis_name ?= redis
network_name ?= rated_network
pip := .venv/bin/pip
pytest := .venv/bin/pytest
precommit := .venv/bin/pre-commit

.DEFAULT_GOAL := help

##@ 🚀  Getting started
.PHONY: run
run: clean start ## Start the project locally in a container

.PHONY: ready
ready: $(pip)  ## Setup a local dev environment (virtualenv)
	@$(precommit) install
	@$(precommit) install --hook-type commit-msg

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ 🐳  Docker helpers
.PHONY: clean
clean: ## Clean up existing containers and network
	@echo "Cleaning up existing containers..."
	@docker stop $(container_name)_$(instance) 2>/dev/null || true
	@docker stop $(redis_name) 2>/dev/null || true
	@docker network rm $(network_name) 2>/dev/null || true
	@echo "Cleanup complete"

.PHONY: start
start: ## Start everything fresh
	@echo "Building application..."
	@docker build -t $(container_tag) .
	@echo "Creating network..."
	@docker network create $(network_name) 2>/dev/null || true
	@echo "Starting application..."
	@docker run -d --rm \
		--name $(container_name)_$(instance) \
		--network $(network_name) \
		$(container_tag)
	@echo "Containers started. Use 'make logs' to see output"

.PHONY: build
build:  ## Build the Docker container
	@docker build -t $(container_tag) .

.PHONY: up
up: ## Start services
	@docker run -d --rm --name $(container_name)_$(instance) $(container_tag)

.PHONY: down
down: clean ## Alias for `clean`

.PHONY: remove
remove: clean ## Stop containers and remove images
	@docker rmi $(container_tag) || true

.PHONY: logs
logs: ## Output container logs
	@docker logs --follow --tail 50 $(container_name)_$(instance)

##@ 🐍  Local dev helpers
$(pip): requirements.txt
	@python3.12 -m venv .venv
	@$(pip) install --upgrade pip
	@$(pip) install -r requirements.txt
	@touch $(pip)

.PHONY: test
test: $(pip) ## Run tests in a local virtualenv, and optionally provide a path to a specific test file or directory
	@$(pytest) $(path) -vv
