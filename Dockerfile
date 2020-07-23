FROM python:3.7-slim

COPY requirements.txt /

RUN apt-get update -y &&\
    apt-get install git -y
RUN pip install -r /requirements.txt