FROM python:2.7-alpine

WORKDIR /usr/src/app

RUN pip install mysql-connector
RUN pip install pika
RUN pip install requests

COPY ./app /usr/src/app

CMD ["python", "-u", "./main.py"]
