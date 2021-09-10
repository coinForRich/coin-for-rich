# Base image
FROM python:3.8.11-slim-buster

# Install essential packages to the base image
RUN apt-get -y update && apt-get -y install postgresql-client gcc libpq-dev tmux

# Copy code
COPY requirements.txt /coin-for-rich/requirements.txt
RUN pip3 install -r /coin-for-rich/requirements.txt
COPY .env /coin-for-rich/.env
COPY celery_app /coin-for-rich/celery_app
COPY commands /coin-for-rich/commands
COPY common /coin-for-rich/common
COPY cron_scripts /coin-for-rich/cron_scripts
COPY fetchers /coin-for-rich/fetchers
COPY tests /coin-for-rich/tests
COPY web /coin-for-rich/web
WORKDIR /coin-for-rich

# Make dirs
RUN mkdir -p /coin-for-rich/logs
