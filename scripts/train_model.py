"""Train fraud model offline and save fraud_model.pkl.

Run inside the containers so dependencies and data paths match:
    docker compose exec worker python scripts/train_model.py --model gbdt
    # or sample to speed up:
    docker compose exec worker python scripts/train_model.py --model gbdt --sample-fraction 0.2

Outputs the artifact to utils/models/fraud_model.pkl (shared volume).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# Keep model features in one place
MODEL_FEATURES = ["amount", "oldbalanceOrg", "newbalanceDest", "isFlaggedFraud", "type"]

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "transactions.csv"
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "utils" / "models" / "fraud_model.pkl"


def load_data(sample_fraction: float | None, sample_rows: int | None) -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Data file missing: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)

    if sample_rows is not None:
        sample_rows = min(sample_rows, len(df))
        df = df.sample(n=sample_rows, random_state=42)
    elif sample_fraction is not None and sample_fraction < 1.0:
        df = df.sample(frac=sample_fraction, random_state=42)

    return df


def build_preprocess() -> ColumnTransformer:
    return ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["type"]),
            ("num", "passthrough", ["amount", "oldbalanceOrg", "newbalanceDest", "isFlaggedFraud"]),
        ],
        remainder="drop",
    )


def build_model(kind: str, preprocess: ColumnTransformer) -> Pipeline:
    if kind == "logreg":
        clf = LogisticRegression(
            max_iter=200,
            class_weight="balanced",
            solver="lbfgs",
            n_jobs=1,
        )
    elif kind == "gbdt":
        clf = GradientBoostingClassifier(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=5,
            random_state=42,
        )
    else:
        raise ValueError(f"Unknown model type: {kind}")

    return Pipeline([("prep", preprocess), ("clf", clf)])


def train(kind: str, sample_fraction: float | None, sample_rows: int | None) -> None:
    print(f"Loading data from {DATA_PATH} ...")
    df = load_data(sample_fraction, sample_rows)
    print(f"Data shape: {df.shape}")

    X = df[MODEL_FEATURES]
    y = df["isFraud"]

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    preprocess = build_preprocess()
    model = build_model(kind, preprocess)

    print(f"Training model: {kind}")
    model.fit(X_train, y_train)

    val_proba = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, val_proba)
    print(f"Validation ROC-AUC: {auc:.4f}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, OUTPUT_PATH)
    print(f"Saved model to {OUTPUT_PATH}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train fraud detection model offline.")
    parser.add_argument("--model", choices=["logreg", "gbdt"], default="gbdt", help="Model type to train.")
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Optional fraction of data to sample for faster training (0<frac<=1).",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="Optional fixed number of rows to sample for faster training.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    train(args.model, args.sample_fraction, args.sample_rows)
