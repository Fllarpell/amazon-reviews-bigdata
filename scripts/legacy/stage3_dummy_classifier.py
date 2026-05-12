import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 3 dummy classifier baseline")
    parser.add_argument("--train-json", default="data/train.json")
    parser.add_argument("--test-json", default="data/test.json")
    parser.add_argument("--metrics-json", default="output/stage3_dummy_metrics.json")
    parser.add_argument("--metrics-txt", default="output/stage3_dummy_metrics.txt")
    return parser.parse_args()


def iter_jsonl(path):
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            yield json.loads(raw)


def read_labels(path):
    labels = []
    for row in iter_jsonl(path):
        label = row.get("label")
        try:
            labels.append(int(label))
        except (TypeError, ValueError):
            continue
    return labels


def majority_label(labels):
    counts = Counter(labels)
    if not counts:
        raise ValueError("No valid labels found in training set")
    top_count = max(counts.values())
    top_labels = sorted([label for label, cnt in counts.items() if cnt == top_count])
    return top_labels[0], counts


def compute_metrics(true_labels, predicted_label):
    total = len(true_labels)
    if total == 0:
        raise ValueError("No valid labels found in test set")

    correct = 0
    confusion = Counter()
    labels_set = set(true_labels)
    labels_set.add(predicted_label)

    for actual in true_labels:
        predicted = predicted_label
        if actual == predicted:
            correct += 1
        confusion[(actual, predicted)] += 1

    accuracy = float(correct) / float(total)

    per_class = {}
    macro_f1_sum = 0.0
    for label in sorted(labels_set):
        tp = confusion[(label, label)]
        fp = 0
        fn = 0
        for other in labels_set:
            if other != label:
                fp += confusion[(other, label)]
                fn += confusion[(label, other)]

        precision = float(tp) / float(tp + fp) if (tp + fp) > 0 else 0.0
        recall = float(tp) / float(tp + fn) if (tp + fn) > 0 else 0.0
        if precision + recall == 0.0:
            f1 = 0.0
        else:
            f1 = 2.0 * precision * recall / (precision + recall)

        per_class[str(label)] = {
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
            "support": int(sum(confusion[(label, other)] for other in labels_set)),
        }
        macro_f1_sum += f1

    macro_f1 = macro_f1_sum / float(len(labels_set))

    confusion_rows = []
    matrix = defaultdict(dict)
    for actual in sorted(labels_set):
        for predicted in sorted(labels_set):
            value = int(confusion[(actual, predicted)])
            matrix[str(actual)][str(predicted)] = value
            if value > 0:
                confusion_rows.append(
                    {"actual": int(actual), "predicted": int(predicted), "count": value}
                )

    return {
        "total_test_rows": int(total),
        "correct_predictions": int(correct),
        "accuracy": round(accuracy, 6),
        "macro_f1": round(macro_f1, 6),
        "per_class": per_class,
        "confusion_matrix": matrix,
        "non_zero_confusion_rows": confusion_rows,
    }


def write_outputs(metrics_json_path, metrics_txt_path, payload):
    metrics_json_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_txt_path.parent.mkdir(parents=True, exist_ok=True)

    with open(metrics_json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)

    lines = [
        "=== Stage 3 Dummy Baseline ===",
        "model=dummy_majority_class",
        "train_rows={0}".format(payload["train_rows"]),
        "test_rows={0}".format(payload["test_rows"]),
        "majority_label={0}".format(payload["majority_label"]),
        "majority_label_count={0}".format(payload["majority_label_count"]),
        "accuracy={0}".format(payload["metrics"]["accuracy"]),
        "macro_f1={0}".format(payload["metrics"]["macro_f1"]),
    ]
    with open(metrics_txt_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main():
    args = parse_args()
    train_path = Path(args.train_json)
    test_path = Path(args.test_json)
    metrics_json_path = Path(args.metrics_json)
    metrics_txt_path = Path(args.metrics_txt)

    if not train_path.is_file():
        raise FileNotFoundError("Train JSON not found: {0}".format(train_path))
    if not test_path.is_file():
        raise FileNotFoundError("Test JSON not found: {0}".format(test_path))

    train_labels = read_labels(train_path)
    test_labels = read_labels(test_path)
    pred_label, train_distribution = majority_label(train_labels)
    metrics = compute_metrics(test_labels, pred_label)

    payload = {
        "model": "dummy_majority_class",
        "train_path": str(train_path),
        "test_path": str(test_path),
        "train_rows": len(train_labels),
        "test_rows": len(test_labels),
        "majority_label": int(pred_label),
        "majority_label_count": int(train_distribution[pred_label]),
        "train_label_distribution": {str(k): int(v) for k, v in sorted(train_distribution.items())},
        "metrics": metrics,
    }
    write_outputs(metrics_json_path, metrics_txt_path, payload)
    print("Dummy baseline completed")
    print("Saved: {0}".format(metrics_json_path))
    print("Saved: {0}".format(metrics_txt_path))


if __name__ == "__main__":
    main()
