# CLEAN DB PHASE ###############################################################################################################################################################################################################
echo '\c template1;\\DELETE FROM access_log;' | psql --username=postgres --host=localhost

# EXTRACT PHASE ###############################################################################################################################################################################################################
echo "Extracting data"

# Download the data from the URL as .gz file
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz

# Unzip .gz file
gunzip web-server-access-log.txt.gz

# Cut fields 1-4 with delimiter '#' in file 'web-server-access-log.txt'
cut -d"#" -f1-4 web-server-access-log.txt > extracted-data.txt

# TRANSFORM PHASE ###############################################################################################################################################################################################################

tr "#" "," < extracted-data.txt > transformed-data.csv

# LOAD PHASE ###############################################################################################################################################################################################################

echo "\c template1;\COPY access_log FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost

# CONFIRMATION PHASE ###############################################################################################################################################################################################################

echo '\c template1; \\SELECT * from access_log;' | psql --username=postgres --host=localhost

# CLEAN UP PHASE ###############################################################################################################################################################################################################
# rm web-server-access-log.txt
rm extracted-data.txt
rm transformed-data.csv
