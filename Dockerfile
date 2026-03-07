FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Le code source est monté en volume — pas copié dans l'image
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8050", "--reload"]
