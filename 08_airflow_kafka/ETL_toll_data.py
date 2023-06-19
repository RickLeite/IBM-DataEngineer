from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import csv

dag_args = {
    "owner": "dummy_owner",
    "start_date": datetime.today(),
    "email": "dummy_email@example.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ETL_toll_data",
    schedule_interval="@daily",
    default_args=dag_args,
    description="Apache Airflow Final Assignment",
)
def mydag():
    unzip_data = BashOperator(
        task_id="unzip_data",
        bash_command="tar -xvf /home/wedivv/airflow/dags/finalassignment/tolldata.tgz -C /home/wedivv/airflow/dags/finalassignment/",
    )

    @task
    def extract_data_from_csv():
        input_file = "/home/wedivv/airflow/dags/finalassignment/vehicle-data.csv"
        output_file = "/home/wedivv/airflow/dags/finalassignment/csv_data.csv"

        fieldnames = ["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"]
        rows = []

        with open(input_file, "r") as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                extracted_row = {
                    "Rowid": row[0],
                    "Timestamp": row[1],
                    "Anonymized Vehicle number": row[2],
                    "Vehicle type": row[3],
                }
                rows.append(extracted_row)

        with open(output_file, "w", newline="") as csv_output:
            writer = csv.DictWriter(csv_output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @task
    def extract_data_from_tsv():
        input_file = "/home/wedivv/airflow/dags/finalassignment/tollplaza-data.tsv"
        output_file = "/home/wedivv/airflow/dags/finalassignment/tsv_data.csv"

        fieldnames = ["Number of axles", "Tollplaza id", "Tollplaza code"]
        rows = []

        with open(input_file, "r") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            for row in reader:
                extracted_row = {
                    "Number of axles": row[0],
                    "Tollplaza id": row[1],
                    "Tollplaza code": row[2],
                }
                rows.append(extracted_row)

        with open(output_file, "w", newline="") as tsv_output:
            writer = csv.DictWriter(tsv_output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @task
    def extract_from_fixed_width():
        input_file = "/home/wedivv/airflow/dags/finalassignment/payment-data.txt"
        output_file = "/home/wedivv/airflow/dags/finalassignment/fixed_width_data.csv"

        field_positions = [
            (58, 62),  # Type of Payment code
            (62, 68),  # Vehicle Code
        ]

        fieldnames = ["Type of Payment code", "Vehicle Code"]

        rows = []

        with open(input_file, "r") as width_fixed_file:
            for line in width_fixed_file:
                extracted_row = {}
                for i, position in enumerate(field_positions):
                    start_pos, end_pos = position
                    field_value = line[start_pos:end_pos].strip()
                    extracted_row[fieldnames[i]] = field_value
                rows.append(extracted_row)

        with open(output_file, "w", newline="") as csv_output:
            writer = csv.DictWriter(csv_output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    consolidate_data_task = BashOperator(
        task_id="consolidate_data",
        bash_command=(
            "paste -d '\n' -s "
            "~/airflow/dags/finalassignment/csv_data.csv "
            "~/airflow/dags/finalassignment/tsv_data.csv "
            "~/airflow/dags/finalassignment/fixed_width_data.csv "
            "> ~/airflow/dags/finalassignment/extracted_data.csv"
        ),
    )

    # fmt: off

    transform_data_task = BashOperator(
        task_id="transform_data",
        bash_command=(
        r"""awk 'BEGIN{FS=","; OFS=","} { $4=toupper($4); print }' """
           " ~/airflow/dags/finalassignment/extracted_data.csv "
           " > ~/airflow/dags/finalassignment/staging/transformed_data.csv "),
    )

    # fmt: on

    (
        unzip_data
        >> [
            extract_data_from_csv(),
            extract_data_from_tsv(),
            extract_from_fixed_width(),
        ]
        >> consolidate_data_task
        >> transform_data_task
    )


mydag()
