DOCKER_IMAGE := us-docker.pkg.dev/grafanalabs-dev/docker-prometheus-cosmosdb-diagnostic-exporter-dev/prometheus-cosmosdb-diagnostic-exporter
DOCKER_TAG := $(shell git rev-parse --short HEAD)

.PHONY: build-binary
build-binary:
	# Statically link binaries, in order to avoid issues with missing libraries
	# if the binary is built on a distribution with different libraries then the runtime.
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-extldflags \"-static\"" -o exporter_linux_amd64 ./cmd
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "-extldflags \"-static\"" -o exporter_linux_arm64 ./cmd

# Manually build and push multi-arch Docker image.
.PHONY: build-and-push-image
build-and-push-image:
	docker buildx create --use --name prometheus-cosmosdb-diagnostic-exporter-builder || true
	docker buildx inspect prometheus-cosmosdb-diagnostic-exporter-builder --bootstrap
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg REVISION=$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		--push .
