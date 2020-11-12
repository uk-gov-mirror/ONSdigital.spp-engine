FROM spp-engine_python:latest

RUN mkdir /opt/workspace
RUN curl -L https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz | \
    tar -xz -C /opt/workspace

RUN apt-get update && apt-get install -y openjdk-8-jdk

ENV SPARK_HOME=/opt/workspace/spark-2.4.6-bin-hadoop2.7
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/python:$PATH
WORKDIR /usr/src/app

