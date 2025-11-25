FROM python:3.11-slim

WORKDIR /app

# Install curl for healthcheck and poetry
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir poetry

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --only main --no-root --no-interaction --no-ansi

# Copy application code
COPY app.py ./
COPY routes/ ./routes/
COPY services/ ./services/

EXPOSE 8004

CMD ["python", "app.py"]
