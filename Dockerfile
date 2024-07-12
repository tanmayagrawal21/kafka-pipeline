# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the working directory contents into the container at /app
COPY . /app

# Set environment variables (you can override these when running the container)
ENV BOOTSTRAP_SERVERS=localhost:29092
ENV GROUP_ID=my-consumer-group
ENV AUTO_OFFSET_RESET=earliest
ENV INPUT_TOPIC=user-login
ENV OUTPUT_TOPIC=processed-user-login
ENV TARGET_APP_VERSION=2.3.0

# Run the consumer_producer.py script when the container launches
CMD ["python", "event_process.py"]
