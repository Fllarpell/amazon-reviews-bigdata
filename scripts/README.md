# scripts

Stage entrypoints live here.

The grader runs stage scripts from repository root, for example:

```bash
bash scripts/stage1.sh
```

For Stage 2:

```bash
bash scripts/stage2.sh
```

Stage 2 uses `beeline` and writes `output/hive_results.txt` plus `output/q1.csv..output/q5.csv`.

To remove generated artifacts before a fresh pipeline run:

```bash
bash scripts/clean_artifacts.sh
```

To remove generated artifacts and raw JSONL files:

```bash
bash scripts/clean_artifacts.sh --with-raw
```

Official Stage III run (Hive feature layer + Spark ML on YARN):

```bash
bash scripts/stage3.sh
```

Collect Stage III run/check logs in `output/logs_stage3`:

```bash
bash scripts/collect_stage3_logs.sh
```

Legacy local helper (not part of official Stage III checklist flow):

```bash
bash scripts/stage3_prep.sh
```

Legacy Stage 3 implementation files are kept in `scripts/legacy/`.
