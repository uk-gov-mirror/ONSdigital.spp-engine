FROM python:3.7-stretch
ARG reqs=dev-
ARG huid=9999

# Create user called "docker" with selected UID
RUN useradd --shell /bin/bash -u $huid -o -c "" -m dockeruser

# create pre-commit cache dir
RUN mkdir -p /home/dockeruser/.cache/pre-commit && \
chown -R dockeruser:dockeruser /home/dockeruser/.cache

RUN apt-get update
RUN apt-get install openjdk-8-jdk -y

RUN curl https://dl.google.com/go/go1.12.6.linux-amd64.tar.gz | \
tar -C /opt -xz && ln -s /opt/go/bin/go /usr/local/bin/go


COPY ${reqs}requirements.txt .
RUN pip install --upgrade pip pipenv && \
pip install -r ${reqs}requirements.txt
ENV SPARK_HOME=/opt/workspace/spark-2.4.6-bin-hadoop2.7
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/python:$PATH
VOLUME /opt/workspace
#USER dockeruser
WORKDIR /usr/src/app

