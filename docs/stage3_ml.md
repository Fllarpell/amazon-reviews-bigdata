# Stage III: Predictive Data Analytics (Spark on YARN)

Official Stage III flow in this repository is:

- `bash scripts/stage3.sh`
- Hive feature source (`sql/stage3_ml_features.hql`)
- Spark execution on YARN (`spark-submit --master yarn`)
- Two tuned models (`model1`, `model2`) + predictions + evaluation

## 1) Hive feature layer

`scripts/stage3.sh` runs the HQL via **beeline** when `HIVE_PASSWORD` is set, with:

- `hive_db_name`, `hive_db_location`, `hive_ml_features_table`, `hive_ml_features_path`

Default feature table: **`ml_features`** in **`HIVE_DB_NAME`** (default `team34_projectdb`).

## 2) Run full Stage III pipeline

```bash
bash scripts/stage3.sh
# or: make stage3-ml
```

The script executes:

1. `sql/stage3_ml_features.hql` (unless `RUN_HIVE_FEATURES=0`)
2. `scripts/stage3_prepare_split.py` via `spark-submit --master yarn`
   - Reads `<HIVE_DB_NAME>.<FEATURE_TABLE>` (defaults: `team34_projectdb` / `ml_features`)
   - Builds `features` + `label` from a non-text feature pipeline:
     - numeric: `helpful_vote`, `price`, `average_rating`, `rating_number`, `review_year`, `review_month`
     - boolean: `verified_purchase` -> numeric binary feature
     - categorical: `main_category` + `store` (`Top-K + other`)
     - encoding: `StringIndexer(handleInvalid=keep)` + `OneHotEncoder(handleInvalid=keep)` + `VectorAssembler`
   - Splits to train/test (70/30)
   - Writes HDFS JSON under `project/data/train` and `project/data/test`
   - Mirrors `data/train.json`, `data/test.json`
3. `scripts/stage3_ml_train.py` via `spark-submit --master yarn`
   - Tuned models: Random Forest (`model1`) and Naive Bayes (`model2`)
   - `CrossValidator` + `ParamGridBuilder` on **train only**
   - Saves models to HDFS: `project/models/model1`, `project/models/model2`
   - Saves predictions (`label`, `prediction`, one partition):
     - `project/output/model1_predictions` and local `output/model1_predictions.csv`
     - `project/output/model2_predictions` and local `output/model2_predictions.csv`
   - Saves comparison dataframe:
     - `project/output/evaluation` and local `output/evaluation.csv`

## 3) Artifacts

- Local:
  - `data/train.json`
  - `data/test.json`
  - `output/model1_predictions.csv`
  - `output/model2_predictions.csv`
  - `output/evaluation.csv`
- HDFS:
  - `project/data/train`, `project/data/test`
  - `project/models/model1`, `project/models/model2`
  - `project/output/model1_predictions`, `project/output/model2_predictions`, `project/output/evaluation`

## 4) Cluster example (team34)

```bash
ssh team34@hadoop-01.uni.innopolis.ru
cd /path/to/amazon-reviews-bigdata
export HIVE_PASSWORD=...   # required for beeline path
bash scripts/stage3.sh
```

Overrides (aligned with `scripts/stage3.sh`):

```bash
HIVE_DB_NAME=team34_projectdb HIVE_ML_FEATURES_TABLE=my_features FEATURE_TABLE=my_features LABEL_COL=label STAGE3_STORE_TOP_K=200 bash scripts/stage3.sh
```

Skip Hive refresh if the feature table already exists:

```bash
RUN_HIVE_FEATURES=0 bash scripts/stage3.sh
```

Legacy helper notes:

- `scripts/stage3_dummy.sh` is deprecated and delegates to `scripts/stage3.sh`.
- Local exploratory helpers are in `scripts/legacy/` (`stage3_prep.sh`, `stage3_data_prep.py`, `stage3_spark_prep.py`, `stage3_dummy_classifier.py`) and are not part of official Stage III checklist execution.

## 5) Feature policy and exclusions

- Text features (`review_text`, `review_title`) are intentionally excluded from the official Stage III flow because the team decided not to use a semantic/NLP recommendation path in this project iteration.
- Identifier columns (`review_id`, `user_id`, `asin`, `parent_asin`) are intentionally excluded from model features to avoid high-cardinality ID leakage and poor generalization.
- The categorical non-text business features (`main_category`, `store`) and transactional flag (`verified_purchase`) are included in the feature extraction pipeline to satisfy checklist expectations on feature engineering.
