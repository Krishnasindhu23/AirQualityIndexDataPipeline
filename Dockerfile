# Dockerfile

# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy entire project into container
COPY . /app

# Install required Python packages
RUN pip install --no-cache-dir pandas mysql-connector-python matplotlib

# Default command (can be overridden by docker-compose)
CMD ["python", "scripts/run_pipeline.py"]
