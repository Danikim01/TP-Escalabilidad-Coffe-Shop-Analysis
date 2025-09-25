#!/usr/bin/env python3
"""Generate docker-compose-scaled.yml based on worker scaling config."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List

# Static metadata for each worker type we know how to generate.
WORKER_DEFINITIONS: Dict[str, Dict[str, object]] = {
    "year_filter": {
        "display_name": "Year Filter Workers",
        "base_service_name": "year-filter-worker",
        "command": ["python", "year_filter_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
    },
    "time_filter": {
        "display_name": "Time Filter Workers",
        "base_service_name": "time-filter-worker",
        "command": ["python", "time_filter_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
    },
    "amount_filter": {
        "display_name": "Amount Filter Workers",
        "base_service_name": "amount-filter-worker",
        "command": ["python", "amount_filter_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
    },
    "results": {
        "display_name": "Results Workers",
        "base_service_name": "results-worker",
        "command": ["python", "results_worker.py"],
        "needs_worker_id": False,
        "supports_prefetch": False,
        "default_prefetch": None,
    },
}

BASE_HEADER = """version: '3.8'\n\nservices:\n  # RabbitMQ Server\n  rabbitmq:\n    image: rabbitmq:3.12-management\n    container_name: rabbitmq-server\n    ports:\n      - \"5672:5672\"\n      - \"15672:15672\"\n    environment:\n      RABBITMQ_DEFAULT_USER: guest\n      RABBITMQ_DEFAULT_PASS: guest\n    networks:\n      - middleware-network\n    healthcheck:\n      test: [\"CMD\", \"rabbitmq-diagnostics\", \"ping\"]\n      interval: 30s\n      timeout: 10s\n      retries: 5\n    volumes:\n      - rabbitmq_data:/var/lib/rabbitmq\n\n  # Gateway Service\n  gateway:\n    build:\n      context: .\n      dockerfile: ./src/gateway/Dockerfile\n    container_name: coffee-gateway\n    ports:\n      - \"12345:12345\"\n    networks:\n      - middleware-network\n    depends_on:\n      rabbitmq:\n        condition: service_healthy\n    environment:\n      - RABBITMQ_HOST=rabbitmq\n      - RABBITMQ_PORT=5672\n    restart: unless-stopped\n\n  # Client Service\n  client:\n    build:\n      context: ./src/client\n      dockerfile: Dockerfile\n    container_name: coffee-client\n    networks:\n      - middleware-network\n    depends_on:\n      - gateway\n      - rabbitmq\n    environment:\n      - GATEWAY_HOST=gateway\n      - GATEWAY_PORT=12345\n    volumes:\n      - ./src/client/.data:/app/.data\n\n"""

FOOTER = """networks:\n  middleware-network:\n    driver: bridge\n\nvolumes:\n  rabbitmq_data:\n"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("workers_config.json"),
        help="Path to the JSON file with worker scaling configuration",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docker-compose-scaled.yml"),
        help="Path where the generated docker-compose file will be written",
    )
    return parser.parse_args()


def read_config(path: Path) -> Dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"Config file not found: {path}") from exc

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc


def load_worker_settings(raw_workers: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    settings: Dict[str, Dict[str, object]] = {}
    for key, meta in WORKER_DEFINITIONS.items():
        worker_raw = raw_workers.get(key)
        if worker_raw is None:
            raise SystemExit(f"Missing configuration for worker '{key}'")

        if isinstance(worker_raw, int):
            count = worker_raw
            worker_cfg: Dict[str, object] = {"count": count}
        elif isinstance(worker_raw, dict):
            worker_cfg = dict(worker_raw)
            count = worker_cfg.get("count")
        else:
            raise SystemExit(
                f"Worker configuration for '{key}' must be an integer or object"
            )

        if not isinstance(count, int) or count < 0:
            raise SystemExit(
                f"Worker '{key}' needs a non-negative integer 'count' (got {count!r})"
            )

        if meta["supports_prefetch"]:
            prefetch = worker_cfg.get("prefetch_count", meta["default_prefetch"])
            if prefetch is None:
                raise SystemExit(
                    f"Worker '{key}' requires a 'prefetch_count' value"
                )
            if not isinstance(prefetch, int) or prefetch <= 0:
                raise SystemExit(
                    f"Worker '{key}' prefetch_count must be a positive integer"
                )
            worker_cfg["prefetch_count"] = prefetch
        settings[key] = worker_cfg
    return settings


def format_environment(environment: Dict[str, str], indent: str = "      ") -> Iterable[str]:
    for key, value in environment.items():
        yield f"{indent}- {key}={value}"


def format_command(command: List[str]) -> str:
    quoted = ", ".join(f'\"{part}\"' for part in command)
    return f"[{quoted}]"


def generate_worker_sections(
    workers: Dict[str, Dict[str, object]],
    common_env: Dict[str, str],
) -> List[str]:
    sections: List[str] = []

    for key, worker_cfg in workers.items():
        meta = WORKER_DEFINITIONS[key]
        count = worker_cfg["count"]
        display_name = meta["display_name"]
        base_service = meta["base_service_name"]
        comment = None
        if count > 0:
            plural = "instancias" if count != 1 else "instancia"
            comment = f"  # {display_name} ({count} {plural})"
        elif count == 0:
            # Skip generating section entirely when count is zero.
            continue

        if comment:
            sections.append(comment)

        for index in range(1, count + 1):
            service_name = base_service
            if count > 1:
                service_name = f"{base_service}-{index}"

            lines = [f"  {service_name}:"]
            lines.append("    build:")
            lines.append("      context: .")
            lines.append("      dockerfile: ./src/workers/Dockerfile")
            lines.append(f"    container_name: {service_name}")
            lines.append("    networks:")
            lines.append("      - middleware-network")
            lines.append("    depends_on:")
            lines.append("      rabbitmq:")
            lines.append("        condition: service_healthy")

            environment: Dict[str, str] = {k: str(v) for k, v in common_env.items()}

            if meta["needs_worker_id"]:
                environment["WORKER_ID"] = str(index)

            if meta["supports_prefetch"]:
                environment["PREFETCH_COUNT"] = str(worker_cfg["prefetch_count"])

            if environment:
                lines.append("    environment:")
                lines.extend(format_environment(environment))

            command = format_command(meta["command"])
            lines.append(f"    command: {command}")
            lines.append("    restart: unless-stopped")

            sections.append("\n".join(lines))

        sections.append("")  # Blank line between worker groups

    return sections


def generate_compose(config: Dict[str, object]) -> str:
    raw_workers = config.get("workers")
    if not isinstance(raw_workers, dict):
        raise SystemExit("Config file must contain a 'workers' object")

    common_env_raw = config.get("common_environment", {})
    if not isinstance(common_env_raw, dict):
        raise SystemExit("'common_environment' must be an object if provided")

    common_env = {k: str(v) for k, v in common_env_raw.items()}
    if not common_env:
        common_env = {"RABBITMQ_HOST": "rabbitmq", "RABBITMQ_PORT": "5672"}

    worker_settings = load_worker_settings(raw_workers)
    worker_sections = generate_worker_sections(worker_settings, common_env)

    compose_parts = [BASE_HEADER.rstrip()]
    if worker_sections:
        compose_parts.append("")
        compose_parts.append("\n".join(worker_sections).rstrip())
        compose_parts.append("")
    compose_parts.append(FOOTER.rstrip())
    compose_parts.append("")
    return "\n".join(compose_parts)


def write_output(path: Path, content: str) -> None:
    path.write_text(content + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    config = read_config(args.config)
    content = generate_compose(config)
    write_output(args.output, content)
    print(f"Generated {args.output} from {args.config}")


if __name__ == "__main__":
    main()
