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

Stage 2 uses `beeline` and writes `output/hive_results.txt` plus `output/q1.csv..output/q4.csv`.

To remove generated artifacts before a fresh pipeline run:

```bash
bash scripts/clean_artifacts.sh
```

To remove generated artifacts and raw JSONL files:

```bash
bash scripts/clean_artifacts.sh --with-raw
```

For ML data preparation before Stage 3:

```bash
bash scripts/stage3_prep.sh
```
