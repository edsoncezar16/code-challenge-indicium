FROM python:3.11.2-bullseye as builder

COPY requirements.txt .

RUN pip install --user -r requirements.txt

FROM python:3.11.2-bullseye 

WORKDIR /pipeline

COPY --from=builder /root/.local /root/.local

ENV PATH=/root/.local:$PATH

COPY ./data ./data

COPY docker-compose.yml .

COPY pipeline.sh . 

RUN chmod +x pipeline.sh

COPY ./scripts ./scripts

#this keeps the container from exiting right after starting. 
CMD [ "/bin/bash", "-c", "while true; do sleep 3600; done" ] 