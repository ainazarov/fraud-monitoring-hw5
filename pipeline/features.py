"""One fitted feature pipeline shared by batch training and online inference.

Only transaction context enters the model. Names, street addresses, identifiers,
merchant names, job titles and the target are deliberately not model features.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


RAW_FEATURES = [
    "transaction_time", "amount", "population_city", "lat", "lon",
    "merchant_lat", "merchant_lon", "cat_id", "us_state", "gender",
]
NUMERIC_FEATURES = [
    "log_amount", "log_population", "hour_sin", "hour_cos", "weekday_sin",
    "weekday_cos", "customer_merchant_km",
]
CATEGORICAL_FEATURES = ["category", "state", "gender"]
FEATURE_SCHEMA_VERSION = 1


class TransactionFeatures(BaseEstimator, TransformerMixin):
    """Read raw fields and derive context features; never fit using labels.

    This stateless transformation is saved inside the fitted sklearn Pipeline,
    so the scoring service executes exactly the same operations as training.
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        frame = X.copy() if isinstance(X, pd.DataFrame) else pd.DataFrame(X)
        missing = sorted(set(RAW_FEATURES) - set(frame.columns))
        if missing:
            raise ValueError(f"Missing transaction features: {missing}")
        timestamp = pd.to_datetime(frame["transaction_time"], errors="raise", utc=True)
        if timestamp.isna().any():
            raise ValueError("transaction_time cannot be null")
        amount = pd.to_numeric(frame["amount"], errors="coerce")
        population = pd.to_numeric(frame["population_city"], errors="coerce")
        hour = timestamp.dt.hour + timestamp.dt.minute / 60.0
        weekday = timestamp.dt.dayofweek + hour / 24.0
        lat1 = np.deg2rad(pd.to_numeric(frame["lat"], errors="coerce"))
        lon1 = np.deg2rad(pd.to_numeric(frame["lon"], errors="coerce"))
        lat2 = np.deg2rad(pd.to_numeric(frame["merchant_lat"], errors="coerce"))
        lon2 = np.deg2rad(pd.to_numeric(frame["merchant_lon"], errors="coerce"))
        haversine = (
            np.sin((lat2 - lat1) / 2.0) ** 2
            + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2.0) ** 2
        )
        result = pd.DataFrame({
            "log_amount": np.log1p(amount.clip(lower=0)),
            "log_population": np.log1p(population.clip(lower=0)),
            "hour_sin": np.sin(2 * np.pi * hour / 24.0),
            "hour_cos": np.cos(2 * np.pi * hour / 24.0),
            "weekday_sin": np.sin(2 * np.pi * weekday / 7.0),
            "weekday_cos": np.cos(2 * np.pi * weekday / 7.0),
            "customer_merchant_km": 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(haversine, 0, 1))),
        }, index=frame.index)
        for original, derived in [("cat_id", "category"), ("us_state", "state"), ("gender", "gender")]:
            result[derived] = frame[original].fillna("unknown").astype(str).str.strip().str.lower()
            result[derived] = result[derived].replace("", "unknown")
        return result


def build_pipeline(random_state=42):
    """Return an unfitted CPU pipeline; fit imputers/encoders on train only."""
    preprocess = ColumnTransformer([
        ("numeric", SimpleImputer(strategy="median", add_indicator=True), NUMERIC_FEATURES),
        ("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
    ])
    return Pipeline([
        ("derive_context", TransactionFeatures()),
        ("encode", preprocess),
        ("model", RandomForestClassifier(
            n_estimators=50, max_depth=10, min_samples_leaf=2,
            class_weight="balanced_subsample", random_state=random_state,
            n_jobs=int(os.environ.get("TRAIN_N_JOBS", "2")),
        )),
    ])
