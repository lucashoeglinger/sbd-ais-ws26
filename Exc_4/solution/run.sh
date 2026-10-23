#!/bin/sh

# CREATE postgres container
docker network create app-net 2>/dev/null || true
docker volume create pgdata 2>/dev/null || true

docker run -d \
    --name exc_db\
    --network app-net\
    --env-file debug.env\
    -v pgdata:/var/lib/postgresql/18/docker\
    postgres:18

docker build -t orderservice .
docker run --name orderservice --network app-net --env-file debug.env orderservice

# todo
# docker build
# docker run db
# docker run orderservice