.PHONY: build
build:  ## Build the Docker container
	@docker compose build

.PHONY: up
up: ## Start services
	@docker compose up -d --force-recreate

.PHONY: logs
logs: ## Output container logs. For one specific service use services variable. Example: `make logs services="app"
	@docker compose logs -f --tail 50 $(services)

.PHONY: install
install:  ## Install dependencies in Docker
	@docker compose run --rm indexer pip install -r requirements.txt

.PHONY: ready
ready: ## Get ready to rumble
	@pre-commit install
	@pre-commit install --hook-type commit-msg

.PHONY: test
test:  ## Run tests in Docker
	@docker compose run --rm -T --entrypoint pytest indexer $(path)
