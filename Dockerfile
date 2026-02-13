FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    tzdata \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app_week1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py loader.py cleaner.py ./

CMD ["python", "main1.py"]



