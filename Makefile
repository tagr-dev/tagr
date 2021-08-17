IMAGE := tagr-dev-image:1.0
.PHONY: build
build:
	@echo "Building image..."
	@docker build -t ${IMAGE} -f Dockerfile.dev .
	@echo "Building image and opening shell..."
	@docker run --env PYTHONPATH="${PYTHONPATH}:/app" -it -p 8888:8888 -v ${PWD}:/app -w /app ${IMAGE} /bin/bash
