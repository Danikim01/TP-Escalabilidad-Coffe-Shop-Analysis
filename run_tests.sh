#!/bin/bash
if ! docker info > /dev/null 2>&1; then
    echo "Docker no está ejecutándose. Por favor, inicia Docker primero."
    exit 1
fi

docker-compose build test-env

docker-compose run --rm test-env --disable-warnings

