FROM apache/airflow:2.5.2-python3.10

WORKDIR /home

ADD requirements.txt .

RUN pip install -r requirements.txt

COPY ./data ./data