# Spark_Hadoop 

## Online Retail II Data Analysis in Hadoop HDFS.

This project is designed to analyze retail data using Apache Spark and Hadoop HDFS. The data is taken from the "Online Retail II" dataset, which contains information about online store transactions. The project uses Docker to deploy Hadoop and Spark environments.

1. Project Structure 
```
spark_hadoop/
┣ data/
┃ ┣ online+retail+ii/
┃ ┣ DATA.md
┃ ┣ merge.ipynb
┃ ┗ online+retail+ii.zip
┣ src/
┃ ┣ requirements.txt
┃ ┣ retail_analysis.py
┣ Dockerfile
┣ README.md
┣ docker-compose-3datanodes.yml
┣ docker-compose.yml
┣ hadoop.env
┗ run_experiments.sh
```

2. How to Use

To run 4 Experiments you need to set the following parameters in `run_experiments.sh`:

```bash
NUM_DATANODES=1 # Number of Datanodes
WITH_OPT=False # Set True if you want to enable optimization 
CLEAN_UP=False  # Set True if you want to enable cleaning
```

After setting up the parameters, run the following commands in the terminal:

```bash
chmod +x run_experiments.sh  
./run_experiments.sh
```

## Results
The experimental results are presented below:

| Experiments | 1DataNode | 3DataNodes |
|----------|:-------------:|:------:|
| W/O Optimization | TIME:26.26sec; RAM:116.96MB | TIME:26.76sec; RAM:117.00MB |
| WITH Oprimization| TIME:21.78sec; RAM:116.76MB | TIME:22.21sec; RAM:115.93MB |

### Detailed information about each stage of execution and memory usage
EXP: 1 DataNode, W/O Optimization
```
2025-04-19 08:47:25,862 - INFO - Spark Application total execution time: 26.26 seconds
2025-04-19 08:47:25,862 - INFO - --- Performance Measurements ---
2025-04-19 08:47:25,862 - INFO - Time taken for 'Read HDFS': 13.29 seconds
2025-04-19 08:47:25,863 - INFO - Time taken for 'Data Filtering ': 0.21 seconds
2025-04-19 08:47:25,863 - INFO - Time taken for 'Aggregate Country Counts - Iteration 1': 2.58 seconds
2025-04-19 08:47:25,863 - INFO - Time taken for 'Aggregate Country Counts - Iteration 2': 1.18 seconds
2025-04-19 08:47:25,863 - INFO - Time taken for 'Aggregate Country Counts - Iteration 3': 1.15 seconds
2025-04-19 08:47:25,863 - INFO - Time taken for 'Calculate Avg Spending': 1.54 seconds
2025-04-19 08:47:25,863 - INFO - Time taken for 'Plot Generation': 4.83 seconds
2025-04-19 08:47:25,863 - INFO - --- RAM Usage Measurements ---
2025-04-19 08:47:25,863 - INFO - RAM usage after 'Read HDFS': 112.25 MB
2025-04-19 08:47:25,863 - INFO - RAM usage after 'Data Filtering ': 112.25 MB
2025-04-19 08:47:25,865 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 1': 112.25 MB
2025-04-19 08:47:25,865 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 2': 113.71 MB
2025-04-19 08:47:25,865 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 3': 113.71 MB
2025-04-19 08:47:25,865 - INFO - RAM usage after 'Calculate Avg Spending': 113.71 MB
2025-04-19 08:47:25,865 - INFO - RAM usage after 'Plot Generation': 140.79 MB
2025-04-19 08:47:25,865 - INFO - Spark Application total RAM usage: 116.96 MB
2025-04-19 08:47:26,478 - INFO - Spark Application finished.
```
EXP: 1 DataNode, With Optimization
```
2025-04-19 08:48:24,407 - INFO - Spark Application total execution time: 21.78 seconds
2025-04-19 08:48:24,408 - INFO - --- Performance Measurements ---
2025-04-19 08:48:24,408 - INFO - Time taken for 'Read HDFS': 11.22 seconds
2025-04-19 08:48:24,408 - INFO - Time taken for 'Data Filtering ': 0.18 seconds
2025-04-19 08:48:24,408 - INFO - Time taken for 'Aggregate Country Counts - Iteration 1': 4.44 seconds
2025-04-19 08:48:24,408 - INFO - Time taken for 'Aggregate Country Counts - Iteration 2': 0.45 seconds
2025-04-19 08:48:24,408 - INFO - Time taken for 'Aggregate Country Counts - Iteration 3': 0.35 seconds
2025-04-19 08:48:24,408 - INFO - Time taken for 'Calculate Avg Spending': 2.47 seconds
2025-04-19 08:48:24,408 - INFO - Time taken for 'Plot Generation': 2.18 seconds
2025-04-19 08:48:24,408 - INFO - --- RAM Usage Measurements  ---
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Read HDFS': 112.23 MB
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Data Filtering ': 112.23 MB
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 1': 112.23 MB
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 2': 113.69 MB
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 3': 113.69 MB
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Calculate Avg Spending': 113.69 MB
2025-04-19 08:48:24,408 - INFO - RAM usage after 'Plot Generation': 139.59 MB
2025-04-19 08:48:24,408 - INFO - Spark Application total RAM usage: 116.76 MB
2025-04-19 08:48:25,005 - INFO - Spark Application finished.
```
EXP: 3 DataNodes, W/O Optimization
```
2025-04-19 08:31:15,203 - INFO - Spark Application total execution time: 26.76 seconds
2025-04-19 08:31:15,203 - INFO - --- Performance Measurements ---
2025-04-19 08:31:15,203 - INFO - Time taken for 'Read HDFS': 13.26 seconds
2025-04-19 08:31:15,203 - INFO - Time taken for 'Data Filtering ': 0.11 seconds
2025-04-19 08:31:15,203 - INFO - Time taken for 'Aggregate Country Counts - Iteration 1': 2.25 seconds
2025-04-19 08:31:15,203 - INFO - Time taken for 'Aggregate Country Counts - Iteration 2': 1.55 seconds
2025-04-19 08:31:15,203 - INFO - Time taken for 'Aggregate Country Counts - Iteration 3': 1.22 seconds
2025-04-19 08:31:15,204 - INFO - Time taken for 'Calculate Avg Spending': 1.62 seconds
2025-04-19 08:31:15,204 - INFO - Time taken for 'Plot Generation': 5.32 seconds
2025-04-19 08:31:15,204 - INFO - --- RAM Usage Measurements  ---
2025-04-19 08:31:15,204 - INFO - RAM usage after 'Read HDFS': 112.52 MB
2025-04-19 08:31:15,205 - INFO - RAM usage after 'Data Filtering ': 112.52 MB
2025-04-19 08:31:15,205 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 1': 112.52 MB
2025-04-19 08:31:15,205 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 2': 113.29 MB
2025-04-19 08:31:15,206 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 3': 113.29 MB
2025-04-19 08:31:15,206 - INFO - RAM usage after 'Calculate Avg Spending': 113.29 MB
2025-04-19 08:31:15,206 - INFO - RAM usage after 'Plot Generation': 141.55 MB
2025-04-19 08:31:15,206 - INFO - Spark Application total RAM usage: 117.00 MB
2025-04-19 08:31:15,736 - INFO - Spark Application finished.
```
EXP: 3 DataNodes, WITH Optimization

```
2025-04-19 08:32:37,455 - INFO - Spark Application total execution time: 22.21 seconds
2025-04-19 08:32:37,455 - INFO - --- Performance Measurements ---
2025-04-19 08:32:37,456 - INFO - Time taken for 'Read HDFS': 11.20 seconds
2025-04-19 08:32:37,456 - INFO - Time taken for 'Data Filtering ': 0.23 seconds
2025-04-19 08:32:37,456 - INFO - Time taken for 'Aggregate Country Counts - Iteration 1': 4.70 seconds
2025-04-19 08:32:37,456 - INFO - Time taken for 'Aggregate Country Counts - Iteration 2': 0.41 seconds
2025-04-19 08:32:37,456 - INFO - Time taken for 'Aggregate Country Counts - Iteration 3': 0.31 seconds
2025-04-19 08:32:37,456 - INFO - Time taken for 'Calculate Avg Spending': 2.46 seconds
2025-04-19 08:32:37,456 - INFO - Time taken for 'Plot Generation': 2.21 seconds
2025-04-19 08:32:37,456 - INFO - --- RAM Usage Measurements  ---
2025-04-19 08:32:37,456 - INFO - RAM usage after 'Read HDFS': 111.42 MB
2025-04-19 08:32:37,456 - INFO - RAM usage after 'Data Filtering ': 111.42 MB
2025-04-19 08:32:37,456 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 1': 111.42 MB
2025-04-19 08:32:37,457 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 2': 112.88 MB
2025-04-19 08:32:37,457 - INFO - RAM usage after 'Aggregate Country Counts - Iteration 3': 112.88 MB
2025-04-19 08:32:37,457 - INFO - RAM usage after 'Calculate Avg Spending': 112.88 MB
2025-04-19 08:32:37,457 - INFO - RAM usage after 'Plot Generation': 138.63 MB
2025-04-19 08:32:37,457 - INFO - Spark Application total RAM usage: 115.93 MB
2025-04-19 08:32:37,645 - INFO - Spark Application finished
```