# Use the official Python 3.10 image from DockerHub
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the content of the local src directory to the working directory except the files in .dockerignore
COPY . /app

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH
ENV PYTHONPATH $PYTHONPATH:/app

# Create the /app/logs directory
RUN mkdir -p /app/logs

# Run your Python script when the container launches
CMD ["python", "run_data_collection.py", "-c", "conf/conf.json", "-l", "/app/logs/run_data_collection.log"]
