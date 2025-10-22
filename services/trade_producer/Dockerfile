FROM python:3.12-slim

ENV PYTHONPATH=/app

RUN pip install poetry==1.8.3

WORKDIR /app

COPY . /app

RUN poetry install

CMD ["poetry", "run", "python", "src/main.py"]