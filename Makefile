IMAGE    := homyscrapy-test
SPIDER   ?= encuentra24
LIMIT    ?= 0
STORAGE  ?= local
NO_PROXY ?= 0

-include .devcontainer/.env
export

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

crawl: ## Run a spider. Opts: SPIDER=mls LIMIT=50 STORAGE=local|gcs NO_PROXY=1
	docker run --rm \
	  -v $(PWD):/app \
	  $(if $(filter-out 1,$(NO_PROXY)),-e PROXY_SERVER=$(PROXY_SERVER) -e PROXY_USER=$(PROXY_USER) -e PROXY_PASSWORD=$(PROXY_PASSWORD)) \
	  $(if $(filter gcs,$(STORAGE)),-e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS)) \
	  $(IMAGE) scrapy crawl $(SPIDER) \
	  $(if $(filter-out 0,$(LIMIT)),-s CLOSESPIDER_ITEMCOUNT=$(LIMIT))
