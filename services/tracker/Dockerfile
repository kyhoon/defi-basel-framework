FROM python:3.11

COPY ./services/tracker/requirements.txt .
RUN pip install -r ./requirements.txt

WORKDIR /src
COPY ./services/tracker/src /src
COPY ./data /src/data

CMD ["python", "main.py"]
