"""Shared training/inference features, versioned reload before each transaction."""
import os
import math
import pandas as pd
from common.metrics import SCORER_METRICS
from pipeline.registry import HotReloadingModel


class FraudScorer:
    def __init__(self, metrics=SCORER_METRICS):
        try:
            self.threshold = float(os.environ.get("FRAUD_THRESHOLD", "0.5"))
        except ValueError as exc:
            raise ValueError("FRAUD_THRESHOLD must be a finite number between 0 and 1") from exc
        if not math.isfinite(self.threshold) or not 0 <= self.threshold <= 1:
            raise ValueError("FRAUD_THRESHOLD must be a finite number between 0 and 1")
        self.model = HotReloadingModel(os.environ.get("STORAGE_DIR", "/opt/project/storage"))
        self.metrics = metrics

    def score(self, data):
        previous_version = self.model.version
        try:
            scores, version = self.model.predict(pd.DataFrame([data]))
        finally:
            # Observe actual in-memory activation, including a successful load
            # followed by a prediction failure. Failed reloads retain version.
            self.metrics.record_model(previous_version, self.model.version, self.model.last_error)
        score = float(scores[0])
        return {"score": score, "fraud_flag": int(score > self.threshold), "model_version": version}
