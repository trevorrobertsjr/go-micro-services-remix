# Define the Docker image and tags
IMAGE_NAME=hotelreservation-aio
DOCKER_HUB_TAG=trevorrobertsjr/$(IMAGE_NAME)
ECR_REGISTRY=318168271290.dkr.ecr.us-east-1.amazonaws.com
ECR_TAG=$(ECR_REGISTRY)/$(IMAGE_NAME)

.PHONY: all build linux docker aws proto data run

# Default target: build and push to Docker Hub
all: build docker

# Section 1: Build the Docker image
build:
	docker buildx build --platform linux/amd64 -t $(IMAGE_NAME):latest .

# If building on Linux
linux:
	docker build -t $(IMAGE_NAME):latest .

# Experimental builds to run on Azure environment
expbuild:
	docker buildx build --platform linux/amd64 -t $(IMAGE_NAME):experimental .

# Experimental builds to run on Azure environment
expdocker:
	docker tag $(IMAGE_NAME):experimental $(DOCKER_HUB_TAG):experimental
	docker push $(DOCKER_HUB_TAG):experimental

# Section 2: Tag and push the Docker image to Docker Hub
docker:
	docker tag $(IMAGE_NAME):latest $(DOCKER_HUB_TAG):latest
	docker push $(DOCKER_HUB_TAG):latest

# Section 3: Tag and push the Docker image to AWS ECR
aws:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_REGISTRY)
	docker tag $(IMAGE_NAME):latest $(ECR_TAG):latest
	docker push $(ECR_TAG):latest


### Original Makefile content
# .PHONY: proto data run

proto:
	for f in services/**/proto/*.proto; do \
		protoc --go_out=plugins=grpc:. $$f; \
		echo compiled: $$f; \
	done

data:
	go-bindata -o data/bindata.go -pkg data data/*.json

run:
	docker-compose build
	docker-compose up --remove-orphans
