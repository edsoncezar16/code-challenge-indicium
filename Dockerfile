FROM apache/airflow:2.5.2

ADD requirements.txt .

RUN pip install -r requirements.txt

COPY ./data ./data

COPY docker-compose.yml .

COPY pipeline.sh . 

RUN chmod +x pipeline.sh

COPY ./scripts ./scripts

#this keeps the container from exiting right after starting. 
CMD [ "/bin/bash", "-c", "while true; do sleep 3600; done" ] 