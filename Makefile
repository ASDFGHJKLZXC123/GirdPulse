.PHONY: up down topics schemas jobs projector stop-apps sim demo-corrupt replay replay-demo verify-crash-recovery e2e-clean

REPLAY_ARGS ?= --from-beginning
DEMO_CORRUPT_ARGS ?=

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

jobs:      ## launch each stream job as its OWN process via the launcher (PID file + log, RUNNING-state readiness)
	@mkdir -p .logs
	@: > .logs/anomaly-job.log
	@: > .logs/rollup-job.log
	streams/gradlew -p streams :anomaly-job:installDist :rollup-job:installDist -q
	# Launch the installed app image directly (not `gradlew run`): the recorded PID is then the real
	# Streams JVM, so `make stop-apps` SIGTERM triggers its shutdown hook (streams.close(), leaving the
	# consumer group) and reliably terminates it — `gradlew run` instead forks the JVM as a Gradle
	# daemon child that the recorded wrapper PID does not own, leaving the descendant alive on stop.
	scripts/run.sh anomaly-job 'grep -q "State transition.*to RUNNING" .logs/anomaly-job.log' -- streams/anomaly-job/build/install/anomaly-job/bin/anomaly-job
	scripts/run.sh rollup-job 'grep -q "State transition.*to RUNNING" .logs/rollup-job.log' -- streams/rollup-job/build/install/rollup-job/bin/rollup-job

projector: ## launch the TypeScript projector after migrations and consumer group join
	@mkdir -p .logs
	@: > .logs/projector.log
	scripts/run.sh projector 'grep -q "projector migrations applied" .logs/projector.log && grep -q "projector consumer joined" .logs/projector.log' -- node --import "$(CURDIR)/projector/node_modules/tsx/dist/loader.mjs" "$(CURDIR)/projector/src/index.ts"

demo-corrupt: ## rebuild with the deliberate coordinate swap enabled
	scripts/demo-corrupt.sh $(DEMO_CORRUPT_ARGS)

replay: ## rebuild cleanly; override with REPLAY_ARGS="--since <ISO> [--to-watermark <file>]"
	scripts/replay.sh $(REPLAY_ARGS)

replay-demo: ## fixed-prefix control -> corrupt -> clean replay with exact checksum proof
	@set -eu; \
		mkdir -p .run; \
		watermark="$(CURDIR)/.run/watermark.json"; \
		control="$(CURDIR)/.run/control-checksum.json"; \
		corrupt="$(CURDIR)/.run/corrupt-checksum.json"; \
		final="$(CURDIR)/.run/final-checksum.json"; \
		scripts/watermark.sh "$$watermark"; \
		scripts/replay.sh --from-beginning --to-watermark "$$watermark"; \
		pnpm --dir projector exec tsx src/tools/checksum.ts --write "$$control"; \
		scripts/demo-corrupt.sh --to-watermark "$$watermark"; \
		pnpm --dir projector exec tsx src/tools/checksum.ts --write "$$corrupt" --expect-different "$$control"; \
		scripts/replay.sh --from-beginning --to-watermark "$$watermark"; \
		pnpm --dir projector exec tsx src/tools/checksum.ts --write "$$final" --expect "$$control"; \
		echo "replay-demo: PASS clean replay exactly matches the fixed-watermark control"

verify-crash-recovery: ## kill -9 a partial replay, resume from Postgres offsets, and verify the control checksum
	scripts/verify-crash-recovery.sh

stop-apps: ## kill every PID in .run/, remove the PID files; safe when nothing runs
	scripts/stop-apps.sh

e2e-clean: stop-apps ## guarantees no stale Kafka/Postgres state
	docker compose down -v
