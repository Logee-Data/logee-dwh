FROM python:3.8.13-slim-buster

RUN apt-get update && apt-get -y install build-essential

ADD scripts/data_cataloger/pip-requirements.txt /pip-requirements.txt
RUN pip install -r /pip-requirements.txt
