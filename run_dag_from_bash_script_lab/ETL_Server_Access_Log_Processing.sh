#!/bin/bash

echo "Extract data"
# Potentially need the data source found below at 
# https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt

cut -d"#" -f1,4 /home/project/airflow/dags/web-server-access-log.txt > /home/project/airflow/dags/extracted.txt

echo "Transform data"
tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt

echo "Load data"
tar -czvf log.tar.gz /home/project/airflow/dags/capitalized.txt