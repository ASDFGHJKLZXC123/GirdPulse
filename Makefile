.PHONY: up down topics schemas jobs stop-apps sim e2e-clean

up:        ## start infra
	docker compose up -d --wait

down:
	docker compose down -v

topics:    ## idempotent topic creation — create only if absent (no `|| true`: that would mask real broker failures)
	docker compose exec redpanda sh -c 'rpk topic list | grep -qw fleet.vehicle-events      || rpk topic create fleet.vehicle-events -p 6 -r 1'
	docker compose exec redpanda sh -c 'rpk topic list | grep -qw fleet.anomalies           || rpk topic create fleet.anomalies -p 6 -r 1'
	docker compose exec redpanda sh -c 'rpk topic list | grep -qw fleet.rollups.region-1m   || rpk topic create fleet.rollups.region-1m -p 3 -r 1'

schemas:   ## register schema files to local Schema Registry
	pnpm --dir scripts run schemas

sim:      ## run simulator in the background through launcher (returns after first emit)
	@mkdir -p .logs
	@: > .logs/sim.log
	./scripts/run.sh sim "grep -q 'ready: first event produced' .logs/sim.log" -- pnpm --dir simulator start

jobs:      ## launch stream jobs as their own background processes (PID file + log, RUNNING-state readiness)
	scripts/run.sh anomaly-job 'grep -q "State transition.*to RUNNING" .logs/anomaly-job.log' -- streams/gradlew -p streams :anomaly-job:run

stop-apps: ## kill every PID in .run/, remove the PID files; safe when nothing runs
	@if [ -d .run ]; then \
		for f in .run/*.pid; do \
			[ -e "$$f" ] || continue; \
			pid=$$(cat "$$f"); \
			name=$$(basename "$$f" .pid); \
			if kill -0 "$$pid" 2>/dev/null; then \
				echo "stopping $$name (pid $$pid)"; \
				kill "$$pid" 2>/dev/null || true; \
			fi; \
			rm -f "$$f"; \
		done; \
	fi

e2e-clean: stop-apps ## guarantees no stale Kafka/Postgres state
	docker compose down -v
