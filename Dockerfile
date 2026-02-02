# -------------------------------
# Base image (small + fast)
# -------------------------------
FROM python:3.11-slim

# -------------------------------
# System dependencies (PyMuPDF)
# -------------------------------
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Working directory
# -------------------------------
WORKDIR /app

# -------------------------------
# Copy requirements first
# (better Docker caching)
# -------------------------------
COPY requirements.txt .

# -------------------------------
# Install Python dependencies
# -------------------------------
RUN pip install --no-cache-dir -r requirements.txt

# -------------------------------
# Copy application code
# -------------------------------
COPY . .

# -------------------------------
# Expose FastAPI port
# -------------------------------
EXPOSE 8000

# -------------------------------
# Production command
# -------------------------------
# CMD ["gunicorn", "api:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--timeout", "300"]

CMD ["gunicorn","api:app","-w", "1","-k", "uvicorn.workers.UvicornWorker","--bind", "0.0.0.0:8000","--timeout", "300"]