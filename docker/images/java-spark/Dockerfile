FROM openjdk:8-jre-slim
ARG huid=9999
VOLUME /opt/workspace
RUN mkdir /opt/workspace || ls /opt/workspace
RUN apt-get update
RUN apt-get install curl -y
RUN curl -LO https://apache.mirrors.nublue.co.uk/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz
RUN tar xzvf spark-2.4.6-bin-hadoop2.7.tgz -C /opt/workspace/

# Create user called "docker" with selected UID
RUN useradd --shell /bin/bash -u $huid -o -c "" -m dockeruser
# -- Runtime


USER dockeruser
WORKDIR /opt/workspace