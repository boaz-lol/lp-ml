import os

import mlflow
import pandas as pd
from sklearn.discriminant_analysis import StandardScaler

MLFOW_TRACKING_URI = os.getenv("MLFOW_TRACKING_URI")
MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI")

def set_mlforw_tracking_uri():
    mlflow.set_tracking_uri(uri=MLFOW_TRACKING_URI)


def get_model_from_mlflow(model_uri: str = MLFLOW_MODEL_URI):
    set_mlforw_tracking_uri()
    loaded_model = mlflow.sklearn.load_model(model_uri)
    return loaded_model

def inference_by_puuid(data: pd.DataFrame):
    mlflow.set_tracking_uri(uri="http://13.209.9.231:5000")
    loaded_model = get_model_from_mlflow()
    
    X = data.drop(['_id', 'win', 'match_id', "puuid", "query_game_name", "created_at", 'championId', 'role'], axis=1)
    scaler = StandardScaler()
    X = scaler.fit_transform(X)
    
    predictions = loaded_model.predict_proba(X)
    ml_inference_document = {
        # key-value 추가
        "lp_teer": predictions.tolist()[0][1]
    }
    return ml_inference_document