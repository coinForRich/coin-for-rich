**This file documents comments and examples to troubleshoot possible issues in this project**

# Timescaledb/Postgresql
- Examples of troubleshooting queries to use inside the `psql` CLI inside the app are located [here](../scripts/database/troubleshoot/troubleshoot.sql)
- Configure logging for Postgresql: https://www.postgresql.org/docs/current/runtime-config-logging.html

# Redis
- Run Redis using Docker Compose with option to use password from .env file
    - https://stackoverflow.com/questions/68461172/docker-compose-redis-password-via-environment-variable
        - "env_file allows to set environment variables in the container - while you need them in the environment of docker-compose in order to perform variable substitution for ${REDIS_PASSWORD}."
