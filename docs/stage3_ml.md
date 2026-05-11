# Stage III: Predictive Data Analytics (Spark on YARN)

Stage III in this repository is implemented with distributed Spark ML only:

- No local `python3 scripts/...py` execution for the main ML flow.
- Run with `spark-submit --master yarn ...`.
- Feature source is Hive table built from `sql/stage3_ml_features.hql` (partitioned Parquet; `hiveconf` substitutions).
- Training artifacts are `data/train.json` and `data/test.json` mirrored from HDFS split output.

## 1) Hive feature layer

On the cluster, `scripts/stage3_dummy.sh` runs the HQL via **beeline** when `HIVE_PASSWORD` is set (same pattern as `scripts/stage3.sh`), with:

- `hive_db_name`, `hive_db_location`, `hive_ml_features_table`, `hive_ml_features_path`

Defaults match `scripts/stage3.sh`: table name **`ml_features`** in **`HIVE_DB_NAME`** (default `team34_projectdb`).

## 2) Run full Stage III pipeline

```bash
bash scripts/stage3_dummy.sh
# or: make stage3-ml
```

The script executes:

1. `sql/stage3_ml_features.hql` (unless `RUN_HIVE_FEATURES=0`)
2. `scripts/stage3_prepare_split.py` via `spark-submit --master yarn`
   - Reads `<HIVE_DB_NAME>.<FEATURE_TABLE>` (defaults: `team34_projectdb` / `ml_features`)
   - Builds `features` + `label` from numeric columns
   - Splits to train/test (70/30)
   - Writes HDFS JSON under `project/data/train` and `project/data/test`
   - Mirrors `data/train.json`, `data/test.json`
3. `scripts/stage3_ml_train.py` via `spark-submit --master yarn`
   - Baseline Random Forest (no CV)
   - Tuned models: Random Forest, Naive Bayes, OneVsRest + LinearSVC (SVM)
   - `CrossValidator` + `ParamGridBuilder` on **train only**
   - Predictions CSV (`label`, `prediction`, one partition) and `evaluation.csv`

## 3) Artifacts

- Local: `output/baseline_random_forest_predictions.csv`, `output/model*_predictions.csv`, `output/evaluation.csv`
- HDFS: `project/output/...`, `project/models/...`

## 4) Cluster example (team34)

```bash
ssh team34@hadoop-01.uni.innopolis.ru
cd /path/to/amazon-reviews-bigdata
export HIVE_PASSWORD=...   # required for beeline path
bash scripts/stage3_dummy.sh
```

Overrides (aligned with `scripts/stage3.sh`):

```bash
HIVE_DB_NAME=team34_projectdb HIVE_ML_FEATURES_TABLE=my_features FEATURE_TABLE=my_features LABEL_COL=label bash scripts/stage3_dummy.sh
```

Skip Hive refresh if the feature table already exists:

```bash
RUN_HIVE_FEATURES=0 bash scripts/stage3_dummy.sh
```

Local / CI **dummy** majority-class path (not the YARN ML assignment):

```bash
STAGE3_DUMMY_ONLY=1 bash scripts/stage3_dummy.sh
```
