FROM python:3.8.11

RUN apt-get update
RUN apt-get install default-jdk -y

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

COPY ./app /app
WORKDIR /app

CMD ["python", "main.py"]