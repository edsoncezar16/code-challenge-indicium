FROM python:3.11.2-bullseye as builder

COPY requirements.txt .

RUN pip install --user -r requirements.txt

FROM python:3.11.2-bullseye 

WORKDIR /pipeline

COPY --from=builder /root/.local /root/.local

COPY ./src .

ENV PATH=/root/.local:$PATH

#this keeps the container from immediately stopping
CMD [ "/bin/bash", "-c", "while true; do sleep 3600; done" ]