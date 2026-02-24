from pathlib import Path
import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from common.config import get_settings

settings = get_settings()

MODEL_PATH = Path(__file__).resolve().parent / "models" / "fraud_model.pkl"
MODEL_FEATURES = [
    "amount", "oldbalanceOrg", "newbalanceDest", "isFlaggedFraud", "type"
]
_model: Pipeline | None = None


def get_or_train_model() -> Pipeline:
    global _model
    if _model is not None:
        return _model

    if MODEL_PATH.exists():
        _model = joblib.load(MODEL_PATH)
        return _model
    
    # training seperately
    return {
        "decision": "APPROVE",
        "fraud_score": 0.0,
        "reasons": ["no_model"]
    }





def build_feature_row(transaction: dict) -> pd.DataFrame:

    return pd.DataFrame([{
        "amount":
        float(transaction.get("amount", 0.0)),
        "oldbalanceOrg":
        float(transaction.get("oldbalanceOrg", 0.0)),
        "newbalanceDest":
        float(transaction.get("newbalanceDest", 0.0)),
        "isFlaggedFraud":
        int(transaction.get("isFlaggedFraud", 0)),
        "type":
        transaction.get("type", "UNKNOWN"),
    }])


def decision_from_score(prob: float,
                        transaction: dict) -> tuple[str, list[str]]:
    reasons = []

    if int(transaction.get("isFlaggedFraud", 0)) == 1:
        reasons.append("flagged_by_source")
        return "BLOCK", reasons

    if prob >= 0.8:
        reasons.append("model_high_risk")
        return "BLOCK", reasons
    if prob >= 0.5:
        reasons.append("model_medium_risk")
        return "FLAG", reasons

    reasons.append("model_low_risk")
    return "APPROVE", reasons
