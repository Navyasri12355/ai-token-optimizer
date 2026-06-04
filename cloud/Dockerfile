# cloud/Dockerfile
# =================
# FastAPI image for Azure Container Apps.
# Azure Container Apps free tier: 180k vCPU-seconds/month.

FROM python:3.11-slim

WORKDIR /app

# Java for PySpark
RUN apt-get update && apt-get install -y \
    default-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App source
COPY api/           ./api/
COPY spark/         ./spark/
COPY cloud/         ./cloud/
COPY optimizer.py   .
COPY metrics.py     .
COPY logging_config.py .

# Pre-download NLTK data
RUN python -c "import nltk; nltk.download('stopwords', quiet=True)"

# Azure Container Apps injects PORT (default 8080)
ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
