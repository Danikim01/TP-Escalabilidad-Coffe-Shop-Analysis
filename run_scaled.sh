#!/bin/bash

set -euo pipefail

CONFIG_PATH=${1:-workers_config.json}

scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose-scaled.yml
docker-compose -f docker-compose-scaled.yml up --build -d
