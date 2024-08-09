# Dockerfile

FROM python:3.12.0-slim-bullseye

ENV PYTHONUNBUFFERED=1

ENV PYTHONPATH="/indexer:/indexer/src"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /indexer

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD ["python", "src/main.py"]

CMD ["python", "-m", "bytewax.run", "src.main:main"]
