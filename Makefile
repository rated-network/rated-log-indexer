##@ 🚀  Getting started
.PHONY: run
run: build up ## Ready, set, go!

.PHONY: build
build:  ## Build the Docker container
	@docker compose build

.PHONY: up
up: ## Start services
	@docker compose up -d --force-recreate indexer db redis

.PHONY: down
down: ## Stop services
	@docker compose down

.PHONY: remove
remove: ## Remove containers, images, networks, and volumes
	@docker compose down --rmi all --volumes --remove-orphans

.PHONY: logs
logs: ## Output container logs. For one specific service use services variable. Example: `make logs services="app"
	@docker compose logs -f --tail 50

.PHONY: install
install:  ## Install dependencies in Docker
	@docker compose run --rm indexer pip install -r requirements.txt

.PHONY: ready
ready: ## Get ready to rumble
	@pre-commit install
	@pre-commit install --hook-type commit-msg

.PHONY: test
test:  ## Run tests in Docker, and optionally provide a path to a specific test file or directory
	@docker compose run --rm -T --entrypoint pytest test $(path) -vv; \
	RESULT=$$?; \
	docker compose down; \
	exit $$RESULT
