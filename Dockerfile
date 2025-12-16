FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt scrapyd

COPY . /app

# Configure Scrapyd
RUN mkdir -p /etc/scrapyd
RUN echo "[scrapyd]\nbind_address = 0.0.0.0\nhttp_port = 6800" > /etc/scrapyd/scrapyd.conf

EXPOSE 6800

# Run Scrapyd
CMD ["scrapyd"]
