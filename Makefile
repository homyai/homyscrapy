IMAGE       := homyscrapy-test
REMOTE_IMAGE := us-central1-docker.pkg.dev/datalake-homyai/homyscrapy/homyscrapy:latest
GCP_PROJECT := datalake-homyai
GCP_REGION  := us-central1
SPIDER   ?= encuentra24
LIMIT    ?= 0
STORAGE  ?= local
NO_PROXY ?= 0

-include .devcontainer/.env
export

.PHONY: help build test lint shell crawl list deploy run-job
.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

lint: ## Run ruff linter and format check
	ruff check . && ruff format --check .

build: ## Build the Docker image
	docker build -t $(IMAGE) .

test: ## Run the test suite inside Docker
	docker run --rm -v $(PWD):/app $(IMAGE) python -m pytest tests/ -v

shell: ## Open a bash shell inside the Docker container
	docker run --rm -it -v $(PWD):/app $(IMAGE) bash

list: ## List all available spiders
	docker run --rm -v $(PWD):/app $(IMAGE) scrapy list

deploy: ## Build, push image and update Cloud Run Job
	docker build -t $(REMOTE_IMAGE) .
	docker push $(REMOTE_IMAGE)
	gcloud run jobs update homyscrapy --image=$(REMOTE_IMAGE) --project=$(GCP_PROJECT) --region=$(GCP_REGION)

run-job: ## Trigger the Cloud Run Job immediately (full extraction, all spiders)
	gcloud run jobs execute homyscrapy --project=$(GCP_PROJECT) --region=$(GCP_REGION)

crawl: ## Run a spider locally. Opts: SPIDER=mls LIMIT=50 STORAGE=local|gcs NO_PROXY=1
	docker run --rm \
	  -v $(PWD):/app \
	  -v $(PWD)/.devcontainer:/credentials:ro \
	  $(if $(filter-out 1,$(NO_PROXY)),-e PROXY_SERVER=$(PROXY_SERVER) -e PROXY_USER=$(PROXY_USER) -e PROXY_PASSWORD=$(PROXY_PASSWORD)) \
	  $(if $(filter gcs,$(STORAGE)),-e GOOGLE_APPLICATION_CREDENTIALS=/credentials/datalake-homyai-new.json) \
	  $(IMAGE) scrapy crawl $(SPIDER) \
	  $(if $(filter-out 0,$(LIMIT)),-s CLOSESPIDER_ITEMCOUNT=$(LIMIT))
