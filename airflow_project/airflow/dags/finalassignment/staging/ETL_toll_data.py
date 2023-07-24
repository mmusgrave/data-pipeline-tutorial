import logging
import shutil
import pandas

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

    unzip_data_task = unzip_data()

    @task(task_id="extract_data_from_csv")
    def extract_data_from_csv():
        """
        This task should extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv file and save them into a file named csv_data.csv.

        vehicle-data.csv is a comma-separated values file.
        It has the below 6 fields

        Rowid  - This uniquely identifies each row. This is consistent across all the three files.
        Timestamp - What time did the vehicle pass through the toll gate.
        Anonymized Vehicle number - Anonymized registration number of the vehicle
        Vehicle type - Type of the vehicle
        Number of axles - Number of axles of the vehicle
        Vehicle code - Category of the vehicle as per the toll plaza.

        """

        all_columns = ["row_id", "timestamp", "anonymized_vehicle_number", "vehicle_type", "number_of_axles", "vehicle_code"]
        desired_columns = ["row_id", "timestamp","anonymized_vehicle_number", "vehicle_type"]

        # Option 1
        vehicle_csv_df = pandas.read_csv("vehicle-data.csv", header=None)
        desired_columns_df = vehicle_csv_df[[0, 1, 2, 3]]
        desired_columns_df.to_csv("csv_data.csv", index=False, header=desired_columns)

        # Option 2
        # vehicle_csv_df = pandas.read_csv("vehicle-data.csv", header=None, names=all_columns)
        # vehicle_csv_df.to_csv("csv_data.csv", index=False, columns=desired_columns)

        # Option 3
        # vehicle_csv_df = pandas.read_csv("vehicle-data.csv", header=None, names=all_columns)
        # desired_columns_df = vehicle_csv_df[desired_columns]
        # desired_columns_df.to_csv("csv_data.csv", index=False)

    extract_data_from_csv_task = extract_data_from_csv()


    @task(task_id="extract_data_from_tsv")
    def extract_data_from_tsv():
        """
        This task should extract the fields Number of axles, Tollplaza id, and Tollplaza code from the tollplaza-data.tsv file and save it into a file named tsv_data.csv.

        tollplaza-data.tsv is a tab-separated values file.
        It has the below 7 fields

        Rowid  - This uniquely identifies each row. This is consistent across all the three files.
        Timestamp - What time did the vehicle pass through the toll gate.
        Anonymized Vehicle number - Anonymized registration number of the vehicle
        Vehicle type - Type of the vehicle
        Number of axles - Number of axles of the vehicle
        Tollplaza id - Id of the toll plaza
        Tollplaza code - Tollplaza accounting code.

        """

        all_columns = ["row_id", "timestamp", "anonymized_vehicle_number", "vehicle_type", "number_of_axles", "tollplaza_id", "tollplaza_code"]
        desired_columns = ["number_of_axles", "tollplaza_id", "tollplaza_code"]

        # Option 1
        tollplaza_tsv_df = pandas.read_table("tollplaza-data.tsv", header=None)
        desired_columns_df = tollplaza_tsv_df[[4, 5, 6]]
        desired_columns_df.to_csv("tsv_data.csv", index=False, header=desired_columns)

        # Option 2
        # TODO: TEST
        # tollplaza_tsv_df = pandas.read_table("tollplaza-data.tsv", header=None, names=all_columns)
        # tollplaza_tsv_df.to_csv("tsv_data.csv", index=False, columns=desired_columns)

        # Option 3
        # TODO: TEST
        # tollplaza_tsv_df = pandas.read_table("tollplaza-data.tsv", header=None, names=all_columns)
        # desired_columns_df = tollplaza_tsv_df[desired_columns]
        # desired_columns_df.to_csv("tsv_data.csv", index=False)

    extract_data_from_tsv_task = extract_data_from_tsv()


    @task(task_id="extract_data_from_fixed_width")
    def extract_data_from_fixed_width():
        """
        This task should extract the fields Type of Payment code, and Vehicle Code from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv.

        payment-data.txt is a fixed width file. Each field occupies a fixed number of characters.

        It has the below 7 fields

        Rowid  - This uniquely identifies each row. This is consistent across all the three files.
        Timestamp - What time did the vehicle pass through the toll gate.
        Anonymized Vehicle number - Anonymized registration number of the vehicle
        Tollplaza id - Id of the toll plaza
        Tollplaza code - Tollplaza accounting code.
        Type of Payment code - Code to indicate the type of payment. Example : Prepaid, Cash.
        Vehicle Code -  Category of the vehicle as per the toll plaza.

        """

        all_columns = ["row_id", "timestamp", "anonymized_vehicle_number", "tollplaza_id", "tollplaza_code", "payment_code_type", "vehicle_code"]
        desired_columns = ["payment_code_type", "vehicle_code"]
        colspecs = [(5, 6), (7, 31), (32, 38), (43, 47), (48, 57), (58, 61), (62,-1)]

        # Option 1
        fixed_width_payment_df = pandas.read_fwf("payment-data.txt", colspecs=colspecs, header=None, names=all_columns)
        desired_columns_df = fixed_width_payment_df[desired_columns]
        desired_columns_df.to_csv("fixed_width_data.csv", index=False)

        # Option 2
        # TODO: TEST
        # fixed_width_payment_df = pandas.read_fwf("payment-data.txt", colspecs=colspecs, header=None, names=all_columns)
        # fixed_width_payment_df.to_csv("fixed_width_data.csv", index=False, columns=desired_columns)

    extract_data_from_fixed_width = extract_data_from_fixed_width()


    # DAG dependencies
    """
    Establish initial DAG task dependencies
    The data must be unzipped before any of the files are extracted
    """
    unzip_data_task >> extract_data_from_csv_task

    unzip_data_task >> extract_data_from_tsv_task

    unzip_data_task >> extract_data_from_fixed_width



    @task(task_id="consolidate_data")
    def consolidate_data():
        """
        This task should create a single csv file named extracted_data.csv by combining data from the following files:
        - csv_data.csv
        - tsv_data.csv
        - fixed_width_data.csv

        The final csv file should use the fields in the order given below:

        Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code

        Hint: Use the bash paste command.

        `paste` command merges lines of files.

        Example : `paste file1 file2 > newfile`

        The above command merges the columns of the files file1 and file2 and sends the output to newfile.

        You can use the command `man paste` to explore more.

        For now, you can trust the rows all correspond to the same data. You can also join on Row ID.
        """
        desired_vehicle_columns_df = pandas.read_csv("csv_data.csv")
        desired_tollplaza_columns_df = pandas.read_csv("tsv_data.csv")
        desired_payment_columns_df = pandas.read_csv("fixed_width_data.csv")

        all_desired_dfs = [desired_vehicle_columns_df, desired_tollplaza_columns_df, desired_payment_columns_df]
        combined_dfs = pandas.concat(all_desired_dfs, axis=1) # axis=1 means combining along the horizontal columns (axis=0 combines along the vertical row indices)
        combined_dfs.to_csv("extracted_data.csv", index=False)



    # DAG dependencies
    """
    Establish second layer of DAG dependencies
    All data must be extracted and put into CSVs before it is consolidated
    """
    extract_data_from_csv_task >> consolidate_data

    extract_data_from_tsv_task >> consolidate_data

    extract_data_from_fixed_width >> consolidate_data


    @task(task_id="transform_data")
    def transform_data():
        """
        This task should transform the vehicle_type field in extracted_data.csv into capital letters and save it into a file named transformed_data.csv in the staging directory.
        """
        extracted_data_df = pandas.read_csv("extracted_data.csv")
        extracted_data_df['vehicle_type'] = extracted_data_df['vehicle_type'].apply(str.upper)
        extracted_data_df.to_csv("transform_data.csv", index=False)


    # DAG dependencies
    """
    Establish final DAG dependency
    All data must be consolidated before it is finally transformed
    """
    consolidate_data >> transform_data
