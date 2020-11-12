FROM openjdk:8-jre-slim
ARG huid=9999

RUN apt-get update
RUN apt-get install curl -y
RUN mkdir -p /opt/workspace && \
curl -L https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz | \
tar -xzvC /opt/workspace/

# Create user called "docker" with selected UID
RUN useradd --shell /bin/bash -u $huid -o -c "" -m dockeruser

# -- Runtime

USER dockeruser
WORKDIR /opt/workspace
