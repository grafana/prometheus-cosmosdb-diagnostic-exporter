DOCKER_IMAGE := us-docker.pkg.dev/grafanalabs-dev/docker-prometheus-cosmosdb-diagnostic-exporter-dev/prometheus-cosmosdb-diagnostic-exporter
DOCKER_TAG := $(shell git rev-parse --short HEAD)

.PHONY: build-binary
build-binary:
	# Statically link binaries, in order to avoid issues with missing libraries
	# if the binary is built on a distribution with different libraries then the runtime.
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-extldflags \"-static\"" -o exporter_linux_amd64 ./cmd
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "-extldflags \"-static\"" -o exporter_linux_arm64 ./cmd

# Build and run locally for testing.
.PHONY: run
run:
	go run ./cmd \
		-storage-account-url https://mimirdev10wsdiag.blob.core.windows.net \
		-subscription-id 179c4f30-ebd8-489e-92bc-fb64588dadb3 \
		-storage-resource-group dev-eu-west-2-rg \
		-blob-path-prefix resourceId=/SUBSCRIPTIONS/179C4F30-EBD8-489E-92BC-FB64588DADB3/RESOURCEGROUPS/DEV-EU-WEST-2-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MIMIR-DEV-10

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
