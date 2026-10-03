FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend backend
COPY frontend frontend
COPY training training
COPY data/demo.json data/demo.json
ENV HOST=0.0.0.0
EXPOSE 8000
CMD ["python", "-m", "backend.app"]
