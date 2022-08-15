FROM jupyter/pyspark-notebook:latest

ARG DELTA_CORE_VERSION="2.0.0"

RUN pyspark --packages io.delta:delta-core_2.12:{DELTA_CORE_VERSION} --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

RUN pip install --quiet --no-cache-dir delta-spark==${DELTA_CORE_VERSION} && \
     fix-permissions "${HOME}" && \
     fix-permissions "${CONDA_DIR}"


USER root

RUN echo 'spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension' >> "${SPARK_HOME}/conf/spark-defaults.conf" && \
    echo 'spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog' >> "${SPARK_HOME}/conf/spark-defaults.conf"

# For soda data following https://docs.soda.io/soda-spark/install-and-use.html#requirements
RUN apt update && apt-get -y install unixodbc-dev libsasl2-dev gcc python-dev

USER ${NB_UID}

# Trigger download of delta lake files
RUN echo "from pyspark.sql import SparkSession" > /tmp/init-delta.py && \
    echo "from delta import *" >> /tmp/init-delta.py && \
    echo "spark = configure_spark_with_delta_pip(SparkSession.builder).getOrCreate()" >> /tmp/init-delta.py && \
    python /tmp/init-delta.py && \
    rm /tmp/init-delta.py

# install soda for spark, both legacy soda-sq soda-spark and soda-core,soda-core-spark-df
RUN pip install --no-cache-dir install soda-spark
RUN pip install --no-cache-dir install soda-core-spark-df
RUN pip install --no-cache-dir install faker
