FROM python:3.11

RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    r-base \
    r-base-dev

COPY ./services/tracker/requirements.txt .
RUN pip install -r ./requirements.txt

WORKDIR /src
COPY ./services/tracker/src /src
COPY ./data /src/data

CMD ["python", "main.py"]
