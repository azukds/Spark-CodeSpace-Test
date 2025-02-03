FROM python:3.10

# Install Python, Java, and Git
RUN apt-get update 
RUN apt-get install -y python3 python3-pip openjdk-17-jre-headless git

# Copy the requirements file into the container
COPY requirements.txt .

# Install pip dependencies
RUN pip3 install --timeout=600 -r requirements.txt

# Set environment variables for Spark
# https://spark.apache.org/docs/latest/api/python/getting_started/install.html
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
