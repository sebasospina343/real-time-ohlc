FROM python:3.12-slim

# Install build tools needed for compiling packages like twofish
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry==1.8.3

WORKDIR /app

COPY . /app

RUN poetry install

# Remove build tools to reduce image size
RUN apt-get remove -y gcc g++ make && apt-get autoremove -y && apt-get clean

ENV PYTHONPATH=/app

EXPOSE 80

CMD ["poetry", "run", "gunicorn", "-w", "2", "-b", "0.0.0.0:80", "src.api:app"]