FROM python:3.11-slim

# Системные зависимости для OCR
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-rus \
    poppler-utils \
    libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код приложения
COPY schema.sql .
COPY *.py ./

# Директории
RUN mkdir -p /app/pdfs /app/data

# Переменные среды (переопределяются в docker-compose)
ENV DATABASE_URL=postgresql+asyncpg://sportrank:sportrank@db:5432/sportrank
ENV ALLOWED_ORIGINS=http://localhost:3000
ENV JWT_SECRET=change-me-in-production
ENV PDF_OUTPUT_DIR=/app/pdfs

EXPOSE 8000

CMD ["uvicorn", "admin_api:app", "--host", "0.0.0.0", "--port", "8000"]
