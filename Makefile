DOCKER_TAG ?= "debian_10_daq"
.PHONY: help pull build run pin_requirements notebook venv
.DEFAULT_GOAL := help

pull: ## pull new images
	docker image pull debian:10

build: ## build docker container
	docker build -t $(DOCKER_TAG) .

run: ## run bash in docker container
	docker run --rm -ti  -v $$(pwd):/app/ $(DOCKER_TAG):latest /bin/bash

pin_requirements: ## pin requirements in docker containet
	docker run --rm -ti -v $$(pwd):/app/ $(DOCKER_TAG):latest bash bin/pin_requirements.sh

notebook: ## run jupyter notebook in docker container
	docker run --rm -ti -p 5000:5000 -p 8787:8787 -v $$(pwd):/app/ $(DOCKER_TAG):latest /home/docker/venv/bin/jupyter-lab --ip 0.0.0.0 --port 5000 --no-browser --LabApp.token=''

venv: ## build local virtual environment
	virtualenv -p python3.6  venv

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
