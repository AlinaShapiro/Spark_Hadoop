#!/bin/bash

# Running Hadoop with 1 Namenode and 3 Datanodes:
NUM_DATANODES=1 # Number of Datanodes
WITH_OPT=False # Use Optimization or not 
CLEAN_UP=False  # Set True if you want to enable cleaning

# Initiating the containers
if [ "$NUM_DATANODES" -eq 1 ]; then
  echo "NUM_DATANODES is equal to 1"
  docker-compose -f docker-compose.yml up -d

elif [ "$NUM_DATANODES" -eq 3 ]; then
  echo "NUM_DATANODES is equal to 3"
  docker-compose -f docker-compose-3datanodes.yml up -d

else 
  echo "ERROR: The Number of datanodes can be either 1 or 3. You entered ${NUM_DATANODES}!"
  exit 1
fi

# Uploading dataset in HDFS. More information about the dataset can be found in DATA.md.
docker cp ./data/online+retail+ii/online_retail_II.csv namenode:/tmp/
docker exec -it namenode hdfs dfs -mkdir -p /data/retail_data
docker exec -it namenode hdfs dfs -put /tmp/online_retail_II.csv /data/retail_data/

# Run Spark Application
if [ "$WITH_OPT" = "False" ]; then
  echo "Running WITHOUT optimization"
  docker exec -it spark-master spark-submit --master spark://spark-master:7077 retail_analysis.py
elif [ "$WITH_OPT" = "True" ]; then
  echo "Running WITH optimization"
  docker exec -it spark-master spark-submit --master spark://spark-master:7077 retail_analysis.py -o
else
  echo "ERROR: WITH_OPT must be either 'True' or 'False'. You entered '${WITH_OPT}'!"
  exit 1
fi

# Quitting 
docker-compose -f docker-compose.yml down -v
docker-compose -f docker-compose-3datanodes.yml down -v

# Clean up unused Docker resources if CLEAN_UP is set to True
if [ "$CLEAN_UP" = "True" ]; then
  echo "Cleaning up unused Docker resources..."
  docker system prune -af --volumes
else
  echo "Skipping cleanup of unused Docker resources."
fi

echo "Execution completed."