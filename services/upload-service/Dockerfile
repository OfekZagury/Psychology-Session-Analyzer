FROM python:3.9-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/

# Run the application
# host 0.0.0.0 is essential for Docker networking
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]