ROOT := $(CURDIR)
PYTHON ?= $(shell test -x $(ROOT)/.venv/bin/python && echo $(ROOT)/.venv/bin/python || command -v python3)

.PHONY: install lint docker-up docker-down hadoop-up hadoop-down hadoop-build-sqoop hadoop-wait pipeline pipeline-full pipeline-all stage3-ml migrate verify-migrations revert-last-migration secrets

install:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	$(PYTHON) -m pylint config lib \
		etl/fetch_review_dataset.py \
		etl/validate_staged_csv.py \
		db/load_into_postgres.py \
		db/apply_migrations.py \
		db/verify_migrations.py \
		db/revert_last_migration.py \
		scripts/stage2_spark_eda.py \
		scripts/stage3_data_prep.py

docker-up:
	docker compose up -d

docker-down:
	docker compose down

hadoop-up:
	docker compose -p reviewhdfs -f $(ROOT)/infra/hadoop/docker-compose.yml up -d
	$(ROOT)/infra/hadoop/wait_for_hdfs.sh

hadoop-down:
	docker compose -p reviewhdfs -f $(ROOT)/infra/hadoop/docker-compose.yml down

hadoop-build-sqoop:
	docker build --platform linux/amd64 -t reviewhdfs-sqoop:local -f $(ROOT)/infra/hadoop/sqoop/Dockerfile $(ROOT)/infra/hadoop/sqoop

hadoop-wait:
	$(ROOT)/infra/hadoop/wait_for_hdfs.sh

secrets:
	@test -f $(ROOT)/secrets/.psql.pass || (cp $(ROOT)/secrets/.psql.pass.example $(ROOT)/secrets/.psql.pass && echo "created secrets/.psql.pass from example")

migrate: secrets
	bash -lc 'set -a && [ -f "$(ROOT)/.env" ] && . "$(ROOT)/.env"; set +a && cd "$(ROOT)" && $(PYTHON) db/apply_migrations.py'

verify-migrations: secrets
	bash -lc 'set -a && [ -f "$(ROOT)/.env" ] && . "$(ROOT)/.env"; set +a && cd "$(ROOT)" && $(PYTHON) db/verify_migrations.py'

revert-last-migration: secrets
	@test "$(CONFIRM)" = "yes" || (echo "Set CONFIRM=yes to drop objects from the latest migration and remove its ledger row." && exit 1)
	bash -lc 'set -a && [ -f "$(ROOT)/.env" ] && . "$(ROOT)/.env"; set +a && cd "$(ROOT)" && $(PYTHON) db/revert_last_migration.py --yes'

pipeline: secrets
	bash $(ROOT)/run_pipeline.sh

pipeline-full: secrets
	bash -lc 'set -a && [ -f "$(ROOT)/.env" ] && . "$(ROOT)/.env"; set +a; unset JSONL_LINE_LIMIT; cd "$(ROOT)" && bash bin/run_pipeline.sh'

pipeline-all: secrets docker-up hadoop-up hadoop-build-sqoop
	bash -lc 'set -a && [ -f "$(ROOT)/.env" ] && . "$(ROOT)/.env"; set +a; unset JSONL_LINE_LIMIT SKIP_SQOOP; export USE_DOCKER_HADOOP=1; cd "$(ROOT)" && bash bin/run_pipeline.sh'

stage3-ml:
	bash $(ROOT)/scripts/stage3_dummy.sh
