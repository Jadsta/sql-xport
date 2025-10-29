FROM amazonlinux:latest

# Install useradd and system dependencies
RUN yum -y update && \
    yum -y install shadow-utils python3 python3-pip gcc make && \
    yum clean all

# Create user tdmon
RUN useradd -m -s /bin/bash tdmon

# Set up working directory
WORKDIR /home/tdmon/app

# Copy project files
COPY . /home/tdmon/app

# Change ownership to tdmon
RUN chown -R tdmon:tdmon /home/tdmon/app

# Switch to tdmon user
USER tdmon

# Install Python dependencies and clean up
RUN pip3 install --user --no-cache-dir Flask PyYAML teradatasql pytz tzlocal gunicorn waitress && \
    find /home/tdmon/app -type d -name "__pycache__" -exec rm -rf {} + && \
    find /home/tdmon/app -type f -name "*.pyc" -delete

# Remove build tools if not needed at runtime
USER root
RUN yum -y remove gcc make && yum clean all
USER tdmon

# Expose default Gunicorn port
EXPOSE 8000

# Entrypoint for Gunicorn
CMD ["/usr/bin/python3", "-m", "gunicorn", "-c", "gunicorn_config.py", "sql_exporter:app"]
