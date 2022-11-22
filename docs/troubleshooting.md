**This file documents comments and examples to troubleshoot possible issues in this project**

# Timescaledb/Postgresql
- Examples of troubleshooting queries to use inside the `psql` CLI inside the app are located [here](../scripts/database/troubleshoot/troubleshoot.sql).
- Configure logging for Postgresql: https://www.postgresql.org/docs/current/runtime-config-logging.html.

# Redis
- Run Redis using Docker Compose with option to use password from .env file
    - https://stackoverflow.com/questions/68461172/docker-compose-redis-password-via-environment-variable
        - "`env_file` allows to set environment variables in the container - while you need them in the environment of docker-compose in order to perform variable substitution for `${REDIS_PASSWORD}`".
        - Thus, if we want to run the Redis container with environment variables specified in the `.env` file, we must use the double dollar sign around Redis password in the container command: `$${REDIS_PASSWORD}`

# Docker
- Use environment variables in Docker Compose file
    - https://docs.docker.com/compose/environment-variables/#substitute-environment-variables-in-compose-files
        - "You can set default values for any environment variables referenced in the Compose file, or used to configure Compose, in an environment file named `.env`".
    - See also: https://stackoverflow.com/questions/29377853/how-to-use-environment-variables-in-docker-compose.
    - The same idea from above repeats: the `env_file` instruction allows you to pass env vars into the container (which means you can use the double dollar `$$` prefix to leave the container shell to parse those env vars), while if you want to use env vars in the configs of the Docker Compose file itself, you may need to have a `.env` ready in the root folder or next to the `docker-compose.yml` file.
