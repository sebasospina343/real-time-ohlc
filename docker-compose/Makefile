start-redpanda:
	docker compose -f redpanda.yml up -d

stop-redpanda:
	docker compose -f redpanda.yml down

run-feature-pipeline:
	docker compose -f feature_pipeline.yml up -d

stop-feature-pipeline:
	docker compose -f feature_pipeline.yml down

build-feature-pipeline:
	docker compose -f feature_pipeline.yml build

run-backfill-pipeline: build-backfill-pipeline
	docker compose -f backfill_pipeline.yml up -d

stop-backfill-pipeline:
	docker compose -f backfill_pipeline.yml down