
FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y python3-pip


COPY src/requirements.txt .
COPY src/retail_analysis.py .

RUN pip3 install -r requirements.txt

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
