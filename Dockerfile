FROM python:3.11.9-slim

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN playwright install chromium

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
