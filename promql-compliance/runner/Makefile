.PHONY: test run run-all

DATASET ?= ../datasets/single-rate.yaml
SUITE ?= ../suites/temporal.yaml
REPORT_DIR ?= /tmp/asapquery-backend-promql-reports
REFERENCE_URL ?= http://localhost:19090
BACKEND_URL ?= http://localhost:19091

CASES := \
	single-rate-temporal:../datasets/single-rate.yaml:../suites/temporal.yaml \
	sparse-checkout-temporal:../datasets/sparse-checkout.yaml:../suites/temporal.yaml \
	aggregations:../datasets/aggregations.yaml:../suites/aggregations.yaml \
	aggregations-dense-cadence:../datasets/aggregations-dense-cadence.yaml:../suites/aggregations.yaml

test:
	go test ./...

run:
	go run ./cmd/differential-runner --dataset $(DATASET) --suite $(SUITE) --reference-url $(REFERENCE_URL) --test-url $(BACKEND_URL)

run-all:
	@set -eu; mkdir -p "$(REPORT_DIR)"; result=0; \
	for test_case in $(CASES); do \
		name="$${test_case%%:*}"; rest="$${test_case#*:}"; dataset="$${rest%%:*}"; suite="$${rest#*:}"; \
		echo "Running $$name"; \
		if ! go run ./cmd/differential-runner --dataset "$$dataset" --suite "$$suite" --reference-url "$(REFERENCE_URL)" --test-url "$(BACKEND_URL)" --output "$(REPORT_DIR)/$$name.json"; then result=1; fi; \
	done; exit $$result
