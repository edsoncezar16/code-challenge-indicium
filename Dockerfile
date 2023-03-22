FROM apache/airflow:2.5.2-python3.10

ADD requirements.txt .

RUN pip install -r requirements.txt

COPY ./data ./data

COPY docker-compose-indicium.yml .

COPY pipeline.sh . 

COPY ./scripts ./scripts