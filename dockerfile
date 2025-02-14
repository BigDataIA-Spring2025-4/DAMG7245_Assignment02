# Use Python 3.12 slim image as base with specific platform
FROM --platform=linux/amd64 python:3.12.8-slim

# Set working directory
WORKDIR /app

# Copy requirements first for layer caching
COPY requirement.txt .
COPY .env /app/.env

# Install dependencies
RUN pip install --no-cache-dir -r requirement.txt

# Copy the application code
COPY ./backend /app/backend

# COPY ./frontend /app/frontend
COPY ./services /app/services

# Set environment variables
ENV API_PORT=8080

# Command to run the application
CMD ["uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
