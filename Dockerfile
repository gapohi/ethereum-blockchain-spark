# Use the Apache Spark image with version 3.5.0
FROM apache/spark:3.5.0

# Switch to the root user to install dependencies
USER root

# Set working directory
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the Python dependencies listed in requirements.txt
# --no-cache-dir prevents caching of the installed packages to reduce the image size
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the spark user for security
USER spark