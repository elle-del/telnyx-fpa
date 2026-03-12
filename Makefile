service := fpa-dashboard
tag := red
main_image := registry.internal.telnyx.com/jenkins/$(service):$(tag)

.PHONY: build test

build:
	docker build --no-cache -t $(main_image) .

test:
	@echo "Running health check test..."
	docker run -d --name $(service)-test -p 8080:8080 $(main_image)
	sleep 2
	curl -f http://localhost:8080/health || (docker rm -f $(service)-test && exit 1)
	curl -f http://localhost:8080/ || (docker rm -f $(service)-test && exit 1)
	docker rm -f $(service)-test
	@echo "✅ Tests passed"
