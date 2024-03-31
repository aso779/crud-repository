.PHONY: lint
lint: ## lint src.
	docker run -t --rm -v $$(pwd):/app -w /app golangci/golangci-lint:v1.57.2 golangci-lint run -v