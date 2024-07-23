import os

import joblib
import mlflow
import pandas as pd
from sklearn.discriminant_analysis import StandardScaler

MLFOW_TRACKING_URI = os.getenv("MLFOW_TRACKING_URI")
MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI")
mlflow.set_tracking_uri(uri="http://13.209.9.231:5000")
loaded_model = mlflow.sklearn.load_model(MLFLOW_MODEL_URI)
scaler = StandardScaler()
loaded_scaler = joblib.load("scaler.joblib")

def inference_by_puuid(data: pd.DataFrame) -> float:
    """
    Given a DataFrame with the specified features, returns the predicted LP tier.
    :param data: DataFrame with the specified features
    :return: Predicted LP tier
    """
    X = loaded_scaler.fit_transform(data)
    predictions = loaded_model.predict_proba(data)
    return predictions.tolist()[0][1]