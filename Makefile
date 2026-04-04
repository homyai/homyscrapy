IMAGE := homyscrapy-test
SPIDER ?= encuentra24

.PHONY: help build test shell crawl list
.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the Docker image
	docker build -t $(IMAGE) .

test: ## Run the test suite inside Docker
	docker run --rm -v $(PWD):/app $(IMAGE) python -m pytest tests/ -v

shell: ## Open a bash shell inside the Docker container
	docker run --rm -it -v $(PWD):/app $(IMAGE) bash

list: ## List all available spiders
	docker run --rm -v $(PWD):/app $(IMAGE) scrapy list

crawl: ## Run a spider (default: encuentra24). Override with SPIDER=mls
	docker run --rm \
	  -v $(PWD):/app \
	  -e PROXY_SERVER=$(PROXY_SERVER) \
	  -e PROXY_USER=$(PROXY_USER) \
	  -e PROXY_PASSWORD=$(PROXY_PASSWORD) \
	  -e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
	  $(IMAGE) scrapy crawl $(SPIDER)
