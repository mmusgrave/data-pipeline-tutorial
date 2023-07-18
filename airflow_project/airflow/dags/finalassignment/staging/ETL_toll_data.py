import logging
import shutil

from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

default_args = {
    'owner': 'Michael Musgrave',
    'start_date': days_ago(0),
    'email': ['michael@fake.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
) as dag:

    @task(task_id="unzip_data")
    def unzip_data():
        # Use the downloaded data from the url given and uncompress it into the destination directory.
        # https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
        shutil.unpack_archive("tolldata.tgz")

    @task(task_id="extract_data_from_csv")
    def extract_data_from_csv():
        # This task should extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv file and save them into a file named csv_data.csv.

    @task(task_id="extract_data_from_tsv")
    def extract_data_from_tsv():
        # This task should extract the fields Number of axles, Tollplaza id, and Tollplaza code from the tollplaza-data.tsv file and save it into a file named tsv_data.csv.

    @task(task_id="extract_data_from_fixed_width")
    def extract_data_from_fixed_width():
        # This task should extract the fields Type of Payment code, and Vehicle Code from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv.

    @task(task_id="consolidate_data")
    def consolidate_data():
        # This task should create a single csv file named extracted_data.csv by combining data from the following files:
        # - csv_data.csv
        # - tsv_data.csv
        # - fixed_width_data.csv
        #
        # The final csv file should use the fields in the order given below:
        #
        # Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code
        #
        # Hint: Use the bash paste command.
        #
        # `paste` command merges lines of files.
        #
        # Example : `paste file1 file2 > newfile`
        #
        # The above command merges the columns of the files file1 and file2 and sends the output to newfile.
        #
        # You can use the command `man paste` to explore more.




    @task(task_id="transform_data")
    def transform_data():
        # This task should transform the vehicle_type field in extracted_data.csv into capital letters and save it into a file named transformed_data.csv in the staging directory.
