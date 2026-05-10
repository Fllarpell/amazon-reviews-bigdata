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

Default engine is `beeline` (`STAGE2_ENGINE=beeline`); use `STAGE2_ENGINE=spark` only when Spark+Hive classpath is configured.

For ML data preparation before Stage 3:

```bash
bash scripts/stage3_prep.sh
```
