import logging
from typing import Optional, Tuple

from pika import BlockingConnection, ConnectionParameters, DeliveryMode, PlainCredentials
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPChannelError
from pika.spec import Basic, BasicProperties

__all__ = ["RabbitMQClient"]

logger = logging.getLogger(__name__)


class RabbitMQClient:
    """Base RabbitMQ Client class"""

    def __init__(
        self,
        host: str,
        port: int = 5672,
        virtual_host: str = "/",
        scheme: str = "amqp",
        username: str = "guest",
        password: str = "guest",
    ) -> None:
        self._host = host
        self._port = port
        self._virtual_host = virtual_host
        self._scheme = scheme
        self._username = username
        self._password = password
        self._rabbitmq: BlockingConnection

        self.connect()

    def connect(self, timeout: Optional[float] = 30) -> None:
        logger.debug("Creating connection to '%s' ...", self._host)

        credentials = PlainCredentials(self._username, self._password)
        parameters = ConnectionParameters(
            self._host, self._port, self._virtual_host, credentials, blocked_connection_timeout=timeout
        )
        self._rabbitmq = BlockingConnection(parameters)

        logger.debug("Connected")

    def channel(self) -> BlockingChannel:
        logger.debug("Creating channel")

        channel = self._rabbitmq.channel()

        logger.debug("Created")

        return channel

    def close(self) -> None:
        logger.debug("Closing rabbitmq connection ...")

        self._rabbitmq.close()

        logger.debug("Closed")

        del self._rabbitmq

    def get(
        self, channel: BlockingChannel, queue: str
    ) -> Tuple[Optional[Basic.GetOk], Optional[BasicProperties], Optional[bytes]]:
        if not channel.is_open:
            raise AMQPChannelError("Channel is closed")

        return channel.basic_get(queue)

    def publish(
        self,
        queue: str,
        message: str = "",
        content_type: str = "application/octect-stream",
        delivery_mode: DeliveryMode = DeliveryMode.Persistent,
    ) -> None:
        channel = self.channel()

        if channel.is_open:
            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=message,
                properties=BasicProperties(content_type=content_type, delivery_mode=delivery_mode.value),
            )
        else:
            raise AMQPChannelError("Channel is closed")

        channel.close()
