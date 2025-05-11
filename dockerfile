# Start from your existing hive image
FROM hive_cluster-h2

USER root

RUN apt update && apt install -y python3 python3-pip && apt clean

RUN pip3 install psycopg2-binary pandas pyarrow hdfs sqlalchemy numpy

RUN mkdir /home/hadoop/tmp

USER hadoop
