# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



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
    {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
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
    {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'}
]

DATASET_NAME="landing_09"
TABLE_NAME="cars-com_dataset"

start_task = EmptyOperator(task_id="start_task", dag=dag)


load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example_Galal",
    bucket="ready-project-dataset",
    source_objects=["cars-com_dataset/cars-com_dataset.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    schema_fields=schema_fields,
    source_format='CSV',
    skip_leading_rows =1, # skip Headers 
    ignore_unknown_values=True,
    max_bad_records=100000

)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task