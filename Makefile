DOCKER_TAG ?= "debian_9_macenv"
.PHONY: help build run pin_requirements notebook notebook_export venv pull
.DEFAULT_GOAL := help

build: ## build docker container
	docker build -t $(DOCKER_TAG) .

run: ## run bash in docker container
	docker run --rm -ti  -v $$(pwd):/app/ $(DOCKER_TAG):latest /bin/bash

pin_requirements: ## pin requirements in docker containet
	docker run --rm -ti -v $$(pwd):/app/ $(DOCKER_TAG):latest bash bin/pin_requirements.sh

notebook: ## run jupyter notebook in docker container
	docker run --rm -ti -p 5000:5000 -p 8787:8787 -v $$(pwd):/app/ $(DOCKER_TAG):latest /home/docker/venv/bin/jupyter-lab --ip 0.0.0.0 --port 5000 --no-browser --LabApp.token=''

notebook_export: ## export jupyter notebooks to html
	docker run --rm -ti -v $$(pwd):/app/ $(DOCKER_TAG):latest /home/docker/venv/bin/jupyter nbconvert --output-dir=html  notebooks/*.ipynb

venv: ## make venv for mac
	virtualenv -p python3.6 venv && \
	REQUESTS_CA_BUNDLE=/usr/local/share/ca-certificates/rootca-2016-07.crt venv/bin/pip install  --index https://software.blue-yonder.org/platform_dev/Debian_9 --extra-index https://software.blue-yonder.org/for_dev/Debian_9 -r requirements.in

pull: ## pull new images
	docker image pull hub.z.westeurope.blue-yonder.cloud/by/debian_9_jenkins:latest

venv: ## build local virtual environment
	virtualenv -p python3.6  venv

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
