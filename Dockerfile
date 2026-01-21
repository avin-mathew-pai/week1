FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app_week1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main1.py loader.py cleaner.py ./

CMD ["python", "main1.py"]



