FROM amazonlinux:latest

# Create user tdmon
RUN useradd -m -s /bin/bash tdmon

# Install Python 3, pip, and system dependencies
RUN yum -y update && \
    yum -y install python3 python3-pip gcc make && \
    yum clean all

# Set up working directory
WORKDIR /home/tdmon/app

# Copy project files
COPY . /home/tdmon/app

# Change ownership to tdmon
RUN chown -R tdmon:tdmon /home/tdmon/app

# Switch to tdmon user
USER tdmon

# Install Python dependencies
RUN pip3 install --user Flask PyYAML teradatasql pytz tzlocal gunicorn waitress

# Expose default Gunicorn port
EXPOSE 8000

# Entrypoint for Gunicorn
CMD ["/usr/bin/python3", "-m", "gunicorn", "-c", "gunicorn_config.py", "sql_exporter:app"]
