# ruff: noqa: E402
import os
import sys

# Add the project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from holmes.utils.cert_utils import add_custom_certificate

ADDITIONAL_CERTIFICATE: str = os.environ.get("CERTIFICATE", "")
if add_custom_certificate(ADDITIONAL_CERTIFICATE):
    print("added custom certificate")

# DO NOT ADD ANY IMPORTS OR CODE ABOVE THIS LINE
# IMPORTING ABOVE MIGHT INITIALIZE AN HTTPS CLIENT THAT DOESN'T TRUST THE CUSTOM CERTIFICATE

import argparse
import logging

import colorlog
import uvicorn

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore

from holmes.config import Config
from experimental.a2a.agent_card import HOLMES_AGENT_CARD
from experimental.a2a.server_a2a import HolmesAgentExecutor


def init_logging():
    logging_level = os.environ.get("LOG_LEVEL", "INFO")
    logging_format = "%(log_color)s%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s"
    logging_datefmt = "%Y-%m-%d %H:%M:%S"

    colorlog.basicConfig(
        format=logging_format, level=logging_level, datefmt=logging_datefmt
    )
    logging.getLogger().setLevel(logging_level)

    httpx_logger = logging.getLogger("httpx")
    if httpx_logger:
        httpx_logger.setLevel(logging.WARNING)

    logging.info(f"Logger initialized using {logging_level} log level")


def create_a2a_server(config: Config):
    """Create and configure the A2A server application."""
    # Create the HolmesGPT executor
    executor = HolmesAgentExecutor(config, dal=config.dal)

    # Create task store for managing A2A tasks
    task_store = InMemoryTaskStore()

    # Create request handler
    request_handler = DefaultRequestHandler(
        agent_executor=executor,
        task_store=task_store,
    )

    # Update agent card URL based on host/port (will be set by card_modifier)
    def update_card_url(card, host: str, port: int):
        card.url = f"http://{host}:{port}/"
        return card

    # Create the A2A Starlette application
    server = A2AStarletteApplication(
        agent_card=HOLMES_AGENT_CARD,
        http_handler=request_handler,
    )

    return server


def main():
    parser = argparse.ArgumentParser(description="HolmesGPT A2A Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=9999, help="Port to bind to")
    args = parser.parse_args()

    init_logging()

    # Default to gpt-4.1 if MODEL not set
    if not os.environ.get("MODEL"):
        os.environ["MODEL"] = "gpt-4.1"

    logging.info("Loading HolmesGPT configuration...")
    config = Config.load_from_env()

    logging.info(f"Creating A2A server on {args.host}:{args.port}...")
    server = create_a2a_server(config)

    # Update agent card URL
    HOLMES_AGENT_CARD.url = f"http://{args.host}:{args.port}/"

    logging.info(f"Starting HolmesGPT A2A Server at http://{args.host}:{args.port}")
    logging.info(f"Agent Card available at http://{args.host}:{args.port}/.well-known/agent.json")

    uvicorn.run(
        server.build(),
        host=args.host,
        port=args.port,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s %(levelname)-8s %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                },
            },
            "loggers": {
                "uvicorn": {"handlers": ["default"], "level": "INFO"},
                "uvicorn.error": {"level": "INFO"},
                "uvicorn.access": {"handlers": ["default"], "level": "INFO"},
            },
        },
    )


if __name__ == "__main__":
    main()
