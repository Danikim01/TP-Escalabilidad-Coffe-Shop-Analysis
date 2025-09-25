#!/bin/bash

docker-compose down 2>/dev/null || true
docker-compose -f docker-compose-scaled.yml up --build -d

