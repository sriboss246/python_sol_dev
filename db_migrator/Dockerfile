FROM python:3-slim
WORKDIR /usr/app/src
RUN pip3 install --upgrade pip
COPY requirements.txt requirements.txt
RUN apt-get update && apt-get install -y libpq-dev gcc
RUN pip install -r requirements.txt --no-cache-dir
COPY ./scripts/db_mgtor.py ./
COPY ./conf/*.ini ./
COPY ./conf/*.json ./
