# Base image
FROM python:3.8.11-slim-buster

# Install essential packages
RUN apt-get -y update 
RUN apt-get -y install wget gnupg2 lsb-release gcc libpq-dev procps cron tmux redis-tools
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list
RUN apt-get -y update && apt-get -y install postgresql-client-13
# nano

# Copy code
COPY requirements.txt /coin-for-rich/requirements.txt
RUN pip3 install -r /coin-for-rich/requirements.txt
COPY .env /coin-for-rich/.env
COPY celery_app /coin-for-rich/celery_app
COPY common /coin-for-rich/common
COPY fetchers /coin-for-rich/fetchers
COPY scripts /coin-for-rich/scripts
COPY tests /coin-for-rich/tests
COPY web /coin-for-rich/web
WORKDIR /coin-for-rich

# Wait-for using dockerize
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Make, rename dirs
RUN mkdir -p /coin-for-rich/logs

# Cron
RUN service cron start

# Chmod
RUN chmod 755 /coin-for-rich/scripts/docker/init.sh
