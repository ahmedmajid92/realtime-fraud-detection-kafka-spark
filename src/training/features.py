"""Feature engineering stages, stored inside the saved PipelineModel.

The stream processor applies exactly the same transforms (no train/serve skew):

1. ``SQLTransformer``: ``log_amount = log1p(Amount)``, and hour-of-day from ``Time``
   encoded cyclically as ``hour_sin`` / ``hour_cos`` (so 23:00 and 00:00 are close).
   The raw ``Time`` (seconds since the start of the 2-day collection window) is not a
   feature: it does not generalise to new data.
2. ``VectorAssembler``: V1..V28 + engineered columns, giving ``features_raw``.
3. ``StandardScaler``: zero mean and unit variance, giving ``features``. This matters for
   LR and the linear SVM; tree models are unaffected.
"""

from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import SQLTransformer, StandardScaler, VectorAssembler

from src.common.schemas import AMOUNT_COLUMN, FEATURE_COLUMNS, TIME_COLUMN

FEATURES_RAW_COL = "features_raw"
FEATURES_COL = "features"
SECONDS_PER_HOUR = 3600.0
HOURS_PER_DAY = 24.0


def feature_engineering_sql() -> str:
    """SQL statement of the feature-engineering ``SQLTransformer``."""
    hour = f"PMOD({TIME_COLUMN} / {SECONDS_PER_HOUR}, {HOURS_PER_DAY})"
    angle = f"(2.0 * PI() * {hour} / {HOURS_PER_DAY})"
    return (
        "SELECT *, "
        f"LOG1P({AMOUNT_COLUMN}) AS log_amount, "
        f"SIN{angle} AS hour_sin, "
        f"COS{angle} AS hour_cos "
        "FROM __THIS__"
    )


def build_feature_pipeline() -> Pipeline:
    """Unfitted feature pipeline (fit on the training split only)."""
    return Pipeline(
        stages=[
            SQLTransformer(statement=feature_engineering_sql()),
            VectorAssembler(
                inputCols=list(FEATURE_COLUMNS), outputCol=FEATURES_RAW_COL, handleInvalid="error"
            ),
            StandardScaler(
                inputCol=FEATURES_RAW_COL, outputCol=FEATURES_COL, withMean=True, withStd=True
            ),
        ]
    )
