FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY search_proxy.py .

RUN mkdir -p /var/log/search-proxy

EXPOSE 8088

CMD ["uvicorn", "search_proxy:app", "--host", "0.0.0.0", "--port", "8088"]
