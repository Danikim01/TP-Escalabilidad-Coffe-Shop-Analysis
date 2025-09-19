"""
Middleware package para comunicación con RabbitMQ.
"""

from .middleware_interface import (
    MessageMiddleware,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError
)

from .rabbitmq_middleware import (
    RabbitMQMiddlewareQueue,
    RabbitMQMiddlewareExchange
)

__all__ = [
    'MessageMiddleware',
    'MessageMiddlewareMessageError',
    'MessageMiddlewareDisconnectedError',
    'MessageMiddlewareCloseError',
    'MessageMiddlewareDeleteError',
    'RabbitMQMiddlewareQueue',
    'RabbitMQMiddlewareExchange'
]
