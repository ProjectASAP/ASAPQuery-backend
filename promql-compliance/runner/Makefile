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
	issue-702-one-second:../datasets/issue-702-one-second.yaml:../suites/issue-702-one-second.yaml \
	issue-754:../datasets/issue-754.yaml:../suites/issue-754.yaml

test:
	cargo test --locked -p promql-compliance

run:
	cargo run --locked -p promql-compliance --bin differential-runner -- --dataset $(DATASET) --suite $(SUITE) --reference-url $(REFERENCE_URL) --test-url $(BACKEND_URL) --compose-file $(COMPOSE_FILE) --logs-dir $(LOGS_DIR)

run-all:
	@set -eu; mkdir -p "$(REPORT_DIR)"; rm -f "$(REPORT_DIR)"/*.json "$(REPORT_DIR)"/summary.md; result=0; \
	for test_case in $(CASES); do \
		name="$${test_case%%:*}"; rest="$${test_case#*:}"; dataset="$${rest%%:*}"; suite="$${rest#*:}"; \
		echo "Running $$name"; \
		if ! cargo run --locked -p promql-compliance --bin differential-runner -- --dataset "$$dataset" --suite "$$suite" --reference-url "$(REFERENCE_URL)" --test-url "$(BACKEND_URL)" --compose-file "$(COMPOSE_FILE)" --logs-dir "$(LOGS_DIR)/$$name" --output "$(REPORT_DIR)/$$name.json"; then result=1; fi; \
	done; cargo run --locked -p promql-compliance --bin report-card -- --reports-dir "$(REPORT_DIR)"; exit $$result

.PHONY: benefit
benefit:
	mkdir -p "$(REPORT_DIR)" "$(LOGS_DIR)"
	cargo run --locked -p promql-compliance --bin benefit-runner -- \
		--dataset ../datasets/issue-754.yaml \
		--suite ../suites/issue-754.yaml \
		--compose-file ../docker-compose.yml \
		--compose-file ../docker-compose.benefit.yml \
		--output "$(REPORT_DIR)/issue-754-benefit.json" \
		--logs-dir "$(LOGS_DIR)/issue-754-benefit"
