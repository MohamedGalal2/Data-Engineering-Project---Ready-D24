import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from google.cloud import storage

def print_hello():
    logging.info("Galal")
    return "printed"

dag = DAG(
    dag_id="Galal_Transfer_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

schema_fields = [
    {'name': 'brand', 'type': 'STRING', 'mode': 'Required'},
    {'name': 'model', 'type': 'STRING', 'mode': 'Required'},
    {'name': 'year', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'mileage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'engine', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'engine_size', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'transmission', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'automatic_transmission', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'fuel_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'drivetrain', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'min_mpg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'max_mpg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'damaged', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'first_owner', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'personal_using', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'turbo', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'alloy_wheels', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'adaptive_cruise_control', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'navigation_system', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'power_liftgate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'backup_camera', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'keyless_start', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'remote_start', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'sunroof_or_moonroof', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'automatic_emergency_braking', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'stability_control', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'leather_seats', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'memory_seat', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'third_row_seating', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'apple_car_play_or_android_auto', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'bluetooth', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'usb_port', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'heated_seats', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'interior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'exterior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'price', 'type': 'FLOAT', 'mode': 'Required'}
]
DATASET_NAME = "landing_09"
TABLE_NAME = "cars-com_dataset"
BUCKET_NAME = "ready-project-dataset"
GCS_FILE_PATH = "cars-com_dataset/cars-com_dataset.csv"

start_task = EmptyOperator(task_id="start_task", dag=dag)

# SQL Operator to check if data exists in the BigQuery table
# def check_gcs_file_exists(bucket_name, file_path):
#     """Check if the file exists in GCS and has content."""
#     client = storage.Client()
#     bucket = client.get_bucket(bucket_name)
#     blob = bucket.blob(file_path)

#     if blob.exists():
#         # Check if the file size is greater than 0 (i.e., it contains data)
#         if blob.size > 0:
#             logging.info(f"File {file_path} exists in bucket {bucket_name} and is not empty.")
#             return True
#         else:
#             logging.warning(f"File {file_path} exists in bucket {bucket_name} but is empty.")
#             return False
#     else:
#         logging.warning(f"File {file_path} does not exist in bucket {bucket_name}.")
#         return False
# check_data_exists=PythonOperator(
# task_id="check_gcs_data_exists",
# python_callable=check_gcs_file_exists,
# op_args=[BUCKET_NAME, GCS_FILE_PATH],
# dag=dag,
# )

load_csv = GCSToBigQueryOperator(
    task_id="transfer_gcs_to_bigquery",
    bucket=BUCKET_NAME,
    source_objects=[GCS_FILE_PATH],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    schema_fields=schema_fields,
    source_format='CSV',
    skip_leading_rows=1,  # Skip Headers
    ignore_unknown_values=True,
    max_bad_records=100000,
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

# Set task dependencies
start_task >> load_csv >> end_task
