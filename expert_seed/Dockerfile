# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies including Chrome and Git
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    gnupg \
    unzip \
    graphviz \
    git \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Create directory for data persistence
RUN mkdir -p /app/data

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99

# Volume for persisting data
VOLUME ["/app/data"]

# Make port 80 available to the world outside this container
EXPOSE 80

# Run the full pipeline when the container launches
CMD ["python", "gsa.py"] 