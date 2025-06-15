FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including PostgreSQL client and Java
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    postgresql-client \
    libpq-dev \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create staticfiles directory
RUN mkdir -p staticfiles

# Expose Django port
EXPOSE 8000

# Default command for Django
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
