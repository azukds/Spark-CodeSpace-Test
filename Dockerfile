# Use ubuntu as base image and manually install spark
FROM ubuntu

# Install Python, Java, and Git
RUN apt-get update 
RUN apt-get install -y python3 python3-pip openjdk-11-jre-headless git

# Copy the requirements file into the container
COPY requirements.txt .

# Install pip dependencies
RUN pip3 install -r requirements.txt

# Set environment variables for Spark
# https://spark.apache.org/docs/latest/api/python/getting_started/install.html
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64