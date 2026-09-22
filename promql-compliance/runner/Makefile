.PHONY: test run run-all

DATASET ?= ../datasets/single-rate.yaml
SUITE ?= ../suites/temporal.yaml
REPORT_DIR ?= /tmp/asapquery-backend-promql-reports
REFERENCE_URL ?= http://localhost:19090
BACKEND_URL ?= http://localhost:19091
COMPOSE_FILE ?= ../docker-compose.yml
LOGS_DIR ?= /tmp/asapquery-backend-promql-logs

CASES := \
	single-rate-temporal:../datasets/single-rate.yaml:../suites/temporal.yaml \
	sparse-checkout-temporal:../datasets/sparse-checkout.yaml:../suites/temporal.yaml \
	aggregations:../datasets/aggregations.yaml:../suites/aggregations.yaml \
	aggregations-dense-cadence:../datasets/aggregations-dense-cadence.yaml:../suites/aggregations.yaml \
	issue-702:../datasets/aggregations-dense-cadence.yaml:../suites/issue-702.yaml \
	issue-702-one-second:../datasets/issue-702-one-second.yaml:../suites/issue-702-one-second.yaml

test:
	go test ./...

run:
	go run ./cmd/differential-runner --dataset $(DATASET) --suite $(SUITE) --reference-url $(REFERENCE_URL) --test-url $(BACKEND_URL) --compose-file $(COMPOSE_FILE) --logs-dir $(LOGS_DIR)

run-all:
	@set -eu; mkdir -p "$(REPORT_DIR)"; rm -f "$(REPORT_DIR)"/*.json "$(REPORT_DIR)"/summary.md; result=0; \
	for test_case in $(CASES); do \
		name="$${test_case%%:*}"; rest="$${test_case#*:}"; dataset="$${rest%%:*}"; suite="$${rest#*:}"; \
		echo "Running $$name"; \
		if ! go run ./cmd/differential-runner --dataset "$$dataset" --suite "$$suite" --reference-url "$(REFERENCE_URL)" --test-url "$(BACKEND_URL)" --compose-file "$(COMPOSE_FILE)" --logs-dir "$(LOGS_DIR)/$$name" --output "$(REPORT_DIR)/$$name.json"; then result=1; fi; \
	done; go run ./cmd/report-card --reports-dir "$(REPORT_DIR)"; exit $$result
