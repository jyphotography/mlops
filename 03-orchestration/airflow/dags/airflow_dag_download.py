from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from io import BytesIO
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
import mlflow
import mlflow.sklearn
import pickle
import os
from sklearn.pipeline import Pipeline
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType

def ingest_files(**context):
    # year = 2023
    # month = 3
    # url = (
    #     'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_trip_data'
    #     f'_{year}-{month:02d}.parquet'
    # )
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet'

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(response.text)
    df = pd.read_parquet(BytesIO(response.content))
    df.to_parquet('/tmp/yellow_taxi_2023_03.parquet')
    print(f"Saved to /tmp/yellow_taxi_2023_03.parquet, total records: {len(df)}")

def read_dataframe(filename):
    df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    return df

def process_and_print_size(**context):
    df = read_dataframe('/tmp/yellow_taxi_2023_03.parquet')
    print(f"Filtered DataFrame size: {len(df)}")

def train_model(**context):
    df = read_dataframe('/tmp/yellow_taxi_2023_03.parquet')

    categorical = ['PULocationID', 'DOLocationID']
    # Ensure columns are string type before converting to dicts
    df[categorical] = df[categorical].astype(str)
    train_dicts = df[categorical].to_dict(orient='records')

    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts)

    target = 'duration'
    y_train = df[target].values

    lr = LinearRegression()
    lr.fit(X_train, y_train)

    print(f"Model intercept: {lr.intercept_}")

    # Create a scikit-learn pipeline
    pipeline = Pipeline([
        ('vectorizer', dv),
        ('regressor', lr)
    ])

    # Configure MLflow tracking URI
    mlflow.set_tracking_uri("http://mlflow:5000")

    # Log model with MLflow
    with mlflow.start_run(run_name="Training Run"): # Added run name for easier identification
        # Log parameters (optional)
        mlflow.log_param("intercept", lr.intercept_)
        mlflow.log_param("n_features", X_train.shape[1])

        # Log the scikit-learn pipeline as an MLflow model
        mlflow.sklearn.log_model(pipeline, "model")

        # Calculate and print the size of the saved artifacts
        # We can still print the size of the pipeline saved by MLflow
        # The path argument to log_model specifies the artifact path *within the run*
        # The actual file path on disk is managed by MLflow internally.
        # Let's list artifacts to confirm instead of checking temporary file size
        # dv_size = os.path.getsize(dv_path)
        # lr_size = os.path.getsize(lr_path)
        # total_artifact_size = dv_size + lr_size
        # print(f"Saved DictVectorizer artifact size: {dv_size} bytes")
        # print(f"Saved LinearRegression artifact size: {lr_size} bytes")
        # print(f"Total saved model artifact size: {total_artifact_size} bytes")

        # Clean up temporary files - these are no longer created as separate files
        # os.remove(dv_path)
        # os.remove(lr_path)

        # Get the run ID while the run is still active
        current_run_id = mlflow.active_run().info.run_id
        print(f"MLflow run completed. Run ID: {current_run_id}")

    # In Airflow 3.0, return values are automatically XCom pushed
    # Let's return the run ID for the next task
    return current_run_id

def register_latest_model(**context):
    # Get the run_id from the previous task's XCom
    ti = context['ti']
    run_id = ti.xcom_pull(task_ids='train_linear_regression_model')

    # Ensure the tracking URI is set for this task
    mlflow.set_tracking_uri("http://mlflow:5000")

    if not run_id:
        raise ValueError("Could not retrieve MLflow run_id from previous task.")

    client = MlflowClient()

    # The model was logged with artifact_path='model' in train_model task
    model_uri = f"runs:/{run_id}/model"

    # Register the model. If the model name doesn't exist, it will be created.
    model_name = "nyc-taxi-lr-model"
    try:
        model_details = mlflow.register_model(model_uri, name=model_name)
        print(f"Successfully registered model '{model_name}' version {model_details.version}")

        # Get model size
        run_id_from_details = model_details.run_id
        artifact_path = "model" # This is the artifact_path used in mlflow.sklearn.log_model
        total_size_bytes = 0
        try:
            artifacts = client.list_artifacts(run_id_from_details, artifact_path)
            for artifact in artifacts:
                if artifact.is_dir:
                    # If it's a directory, list its contents and sum their sizes
                    sub_artifacts = client.list_artifacts(run_id_from_details, f"{artifact_path}/{artifact.path}")
                    for sub_artifact in sub_artifacts:
                         if sub_artifact.file_size is not None:
                            total_size_bytes += sub_artifact.file_size
                elif artifact.file_size is not None:
                    total_size_bytes += artifact.file_size

            print(f"Registered model size: {total_size_bytes} bytes")

        except Exception as size_e:
            print(f"Could not retrieve model size: {size_e}")

    except Exception as e:
        print(f"Error registering model: {e}")
        # Handle potential errors, e.g., model name already exists with different case
        # Or if the run_id/model_uri is invalid

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='download_yellow_taxi_data',
    default_args=default_args,
    schedule=None,  # Use 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=['example'],
) as dag:

    download_task = PythonOperator(
        task_id='download_and_save_yellow_taxi',
        python_callable=ingest_files,
    )

    process_task = PythonOperator(
        task_id='process_and_print_size',
        python_callable=process_and_print_size,
    )

    train_model_task = PythonOperator(
        task_id='train_linear_regression_model',
        python_callable=train_model,
    )

    register_model_task = PythonOperator(
        task_id='register_latest_model',
        python_callable=register_latest_model,
    )

    download_task >> process_task >> train_model_task >> register_model_task