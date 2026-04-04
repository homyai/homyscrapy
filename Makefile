IMAGE := homyscrapy-test
SPIDER ?= encuentra24

.PHONY: build test shell crawl list

build:
	docker build -t $(IMAGE) .

test:
	docker run --rm -v $(PWD):/app $(IMAGE) python -m pytest tests/ -v

shell:
	docker run --rm -it -v $(PWD):/app $(IMAGE) bash

list:
	docker run --rm -v $(PWD):/app $(IMAGE) scrapy list

crawl:
	docker run --rm \
	  -v $(PWD):/app \
	  -e PROXY_SERVER=$(PROXY_SERVER) \
	  -e PROXY_USER=$(PROXY_USER) \
	  -e PROXY_PASSWORD=$(PROXY_PASSWORD) \
	  -e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
	  $(IMAGE) scrapy crawl $(SPIDER)
