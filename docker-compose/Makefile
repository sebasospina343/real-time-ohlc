start-redpanda:
	docker compose -f redpanda.yml up -d

stop-redpanda:
	docker compose -f redpanda.yml down

build-feature-pipeline:
	docker compose -f feature_pipeline.yml build

run-feature-pipeline: build-feature-pipeline
	docker compose -f feature_pipeline.yml up -d

stop-feature-pipeline:
	docker compose -f feature_pipeline.yml down

clean-feature-pipeline-state:
	@echo "Cleaning state directories..."
	rm -rf ../services/trade_to_ohlc/state/trade_to_ohlc_live2
	rm -rf ../services/kafka_to_feature_store/state 2>/dev/null || true
	@echo "State directories cleaned!"

reset-consumer-groups:
	@echo "Resetting consumer group offsets..."
	@docker exec redpanda-0 rpk group delete trade_to_ohlc_live2 2>/dev/null || echo "Consumer group trade_to_ohlc_live2 not found or already deleted"
	@echo "Consumer groups reset!"

clean-feature-pipeline: stop-feature-pipeline clean-feature-pipeline-state reset-consumer-groups
	@echo "Feature pipeline stopped, state cleaned, and consumer groups reset!"

# Remove containers and their filesystem (removes all state inside containers)
remove-feature-pipeline-containers:
	@echo "Removing feature pipeline containers..."
	docker compose -f feature_pipeline.yml down --rmi local
	@echo "Containers removed!"

# Remove state from running containers (executes inside container)
remove-state-from-containers:
	@echo "Removing state from running containers..."
	@docker exec trade-to-ohlc rm -rf /app/state 2>/dev/null || echo "Container trade-to-ohlc not running or state already removed"
	@docker exec kafka-to-feature-store rm -rf /app/state 2>/dev/null || echo "Container kafka-to-feature-store not running or state already removed"
	@echo "State removed from containers!"

# Complete cleanup: remove containers, images, local state, and reset consumer groups
clean-all-feature-pipeline: stop-feature-pipeline clean-feature-pipeline-state reset-consumer-groups remove-feature-pipeline-containers
	@echo "Complete cleanup done! All containers, state, and consumer groups removed."

build-backfill-pipeline:
	docker compose -f backfill_pipeline.yml build

run-backfill-pipeline: build-backfill-pipeline
	docker compose -f backfill_pipeline.yml up -d

stop-backfill-pipeline:
	docker compose -f backfill_pipeline.yml down