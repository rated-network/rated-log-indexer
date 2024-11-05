container_tag ?= rated-log-indexer-indexer
container_name ?= rated_log_indexer

##@ 🚀  Getting started
.PHONY: run
run: build up ## Ready, set, go!

.PHONY: build
build:  ## Build the Docker container
	@docker build -t $(container_tag) .

.PHONY: up
up: ## Start services
	@docker run -d --rm --name $(container_name) $(container_tag)

.PHONY: down
down: ## Stop services
	@docker stop $(container_name)

.PHONY: remove
remove: ## Remove containers, images, networks, and volumes
	@docker remove --volumes $(container_tag) || true

.PHONY: logs
logs: ## Output container logs.
	@docker logs --follow --tail 50 $(container_name)

.PHONY: install
install:  ## Install dependencies in Docker
	@docker run --rm $(container_tag) pip install -r requirements.txt

.PHONY: ready
ready: ## Get ready to rumble
	@pre-commit install
	@pre-commit install --hook-type commit-msg

.PHONY: test
test:  ## Run tests in Docker, and optionally provide a path to a specific test file or directory
	@pytest $(path) -vv
