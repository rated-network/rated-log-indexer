# Use the official Python image from the Docker Hub
FROM python:3.12.0-slim-bullseye

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Install dependencies
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install psycopg2-binary
RUN pip install psycopg2-binary

# Set the working directory
WORKDIR /indexer

# Copy the requirements file
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

RUN pytest --version

# Entry point for running tests
CMD ["pytest", "tests"]
