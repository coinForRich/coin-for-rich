# Base image
FROM python:3.8.11-slim-buster

# Install essential packages
RUN apt-get -qy update
RUN apt-get -qy install wget netcat gnupg2 lsb-release gcc libpq-dev procps cron tmux redis-tools
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list
RUN apt-get -y update && apt-get -y install postgresql-client-13

# Change the working dir and copy needed dirs
WORKDIR /coin-for-rich
COPY requirements.txt .
COPY .env .
COPY pytest.ini .
COPY celery_app ./celery_app
COPY common ./common
COPY fetchers ./fetchers
COPY scripts ./scripts
COPY tests ./tests
COPY web ./web

# Finalize the build with py packages, wait-for script and init script
RUN pip3 install -r requirements.txt
RUN wget --quiet https://raw.githubusercontent.com/eficode/wait-for/master/wait-for
RUN mkdir -p ./logs
RUN service cron start
RUN chmod 755 ./scripts/docker/init.sh
RUN chmod 755 wait-for
