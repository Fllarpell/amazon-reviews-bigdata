# Stage III: Predictive Data Analytics (Spark on YARN)

Stage III in this repository is implemented with distributed Spark ML only:

- No local `python3 scripts/...py` execution for the main ML flow.
- Run with `spark-submit --master yarn ...`.
- Feature source is Hive table from `sql/stage3_ml_features.hql`.
- Training artifacts are `data/train.json` and `data/test.json` mirrored from HDFS split output.

## 1) Build Hive feature layer

```bash
hive -hivevar db_name=team34_projectdb -f sql/stage3_ml_features.hql
```

## 2) Run full Stage III pipeline

```bash
bash scripts/stage3_dummy.sh
```

The script executes:

1. `sql/stage3_ml_features.hql` via `hive -f` (enabled by default in runner)
2. `scripts/stage3_prepare_split.py` via `spark-submit --master yarn`
   - Reads `team34_projectdb.stage3_ml_features`
   - Builds `features` + `label`
   - Splits to train/test
   - Writes HDFS JSON:
     - `project/data/train`
     - `project/data/test`
   - Mirrors local artifacts:
     - `data/train.json`
     - `data/test.json`
3. `scripts/stage3_ml_train.py` via `spark-submit --master yarn`
   - Trains baseline model first:
     - baseline Random Forest (default params)
   - Then trains + tunes 3 model families:
     - Random Forest
     - Naive Bayes
     - SVM (One-vs-Rest with LinearSVC)
   - Uses grid search + cross-validation (`CrossValidator`)
   - Saves model predictions to HDFS and local CSV files
   - Saves evaluation comparison table to HDFS and local CSV

## 3) Artifacts

- Local output:
  - `output/baseline_random_forest_predictions.csv`
  - `output/model1_random_forest_predictions.csv`
  - `output/model2_naive_bayes_predictions.csv`
  - `output/model3_svm_ovr_predictions.csv`
  - `output/evaluation.csv`
- HDFS output:
  - `project/output/model1_random_forest_predictions`
  - `project/output/model2_naive_bayes_predictions`
  - `project/output/model3_svm_ovr_predictions`
  - `project/output/evaluation`
  - `project/models/model1_random_forest`
  - `project/models/model2_naive_bayes`
  - `project/models/model3_svm_ovr`

## 4) Cluster run example (team34)

```bash
ssh team34@hadoop-01.uni.innopolis.ru
cd /path/to/amazon-reviews-bigdata
bash scripts/stage3_dummy.sh
```

If table/column names in Hive differ from defaults, pass env overrides:

```bash
HIVE_DB=team34_projectdb FEATURE_TABLE=stage3_ml_features LABEL_COL=label bash scripts/stage3_dummy.sh
```

If the feature table is already created and you want to skip Hive refresh:

```bash
RUN_HIVE_FEATURES=0 bash scripts/stage3_dummy.sh
```
