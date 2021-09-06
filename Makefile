GO ?=go
GO_TEST_FLAGS ?=-race
GO_PACKAGE ?=$(shell $(GO) list -m -f '{{ .Path }}' || echo 'no_package_detected')

.PHONY: help
help: ## show help message
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make <target> \n\ntargets:\n"} \
		/^[$$()% 0-9a-zA-Z_-]+:.*?##/ {	printf "  %-10s %s\n", $$1, $$2 }' \
			$(MAKEFILE_LIST)									

.PHONY: test
test: ## runs unit tests
	$(GO) test $(GO_TEST_FLAGS) .


.PHONY: coverage
coverage: ## generate coverage raport
	$(GO) test -covermode=atomic -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out


.PHONY: benchmark
benchmark: ## runs the benchmarks
	$(GO) test -bench=. -benchtime 4x -run=^$$

.PHONY: profile
profile: ## generates the cpu profile
	$(GO) test -v  -bench=. -benchtime 4x -cpuprofile profile_cpu.out -memprofile profile_mem.out -run=^$$
	$(GO) tool pprof -pdf profile_cpu.out > profile_cpu.pdf
	$(GO) tool pprof -pdf profile_mem.out > profile_mem.pdf

.PHONY: 
vendor: ## vendors the dependencies, run manually
	$(GO) mod vendor