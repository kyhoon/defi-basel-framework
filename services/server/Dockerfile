FROM python:3.11

COPY ./services/server/requirements.txt .
RUN pip install -r ./requirements.txt

WORKDIR /src
COPY ./services/server/src /src
COPY ./data /src/data

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]