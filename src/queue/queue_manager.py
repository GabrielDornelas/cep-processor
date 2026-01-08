"""
RabbitMQ queue manager for rate-limited CEP processing
"""

import json
import time
import threading
from typing import Optional, Callable, Dict, Any
import queue as stdlib_queue

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

import os
from src.utils.logger import setup_logger


class QueueManager:
    """
    RabbitMQ queue manager for processing CEPs with rate limiting.
    Ensures API requests don't exceed rate limits.
    """

    def __init__(
        self,
        rabbitmq_url: Optional[str] = None,
        queue_name: str = "cep_processing",
        rate_limit_per_second: Optional[float] = None,
        prefetch_count: int = 1
    ):
        """
        Initialize the queue manager.

        Args:
            rabbitmq_url: RabbitMQ connection URL (optional, will use environment variables if not provided)
            queue_name: Name of the queue for CEP processing
            rate_limit_per_second: Maximum requests per second (optional, will use environment variables if not provided)
            prefetch_count: Number of unacknowledged messages per consumer
        """
        if rabbitmq_url:
            self.rabbitmq_url = rabbitmq_url
        else:
            # Try RABBITMQ_URL first, then construct from components
            rabbitmq_url = os.getenv('RABBITMQ_URL')
            if rabbitmq_url:
                self.rabbitmq_url = rabbitmq_url
            else:
                host = os.getenv('RABBITMQ_HOST', 'localhost')
                port = os.getenv('RABBITMQ_PORT', '5672')
                user = os.getenv('RABBITMQ_USER', 'guest')
                password = os.getenv('RABBITMQ_PASSWORD', 'guest')
                self.rabbitmq_url = f"amqp://{user}:{password}@{host}:{port}/"
        
        self.queue_name = queue_name
        self.rate_limit_per_second = rate_limit_per_second or float(os.getenv('RATE_LIMIT_PER_SECOND', '5.0'))
        self.prefetch_count = prefetch_count
        self.logger = setup_logger(name="queue_manager")
        
        # Calculate delay between requests
        self.delay_between_requests = 1.0 / self.rate_limit_per_second if self.rate_limit_per_second > 0 else 0
        
        # Connection and channel
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        
        # Processing control
        self._stop_processing = threading.Event()
        self._last_request_time = 0.0
        self._rate_limit_lock = threading.Lock()

    def connect(self) -> bool:
        """
        Connect to RabbitMQ server.

        Returns:
            True if connected successfully, False otherwise
        """
        try:
            self.logger.info(f"Connecting to RabbitMQ: {self.rabbitmq_url}")
            
            parameters = pika.URLParameters(self.rabbitmq_url)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Set QoS to control rate limiting
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            
            # Declare queue (durable to survive broker restarts)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            
            self.logger.info(f"Connected to RabbitMQ. Queue: {self.queue_name}")
            return True
            
        except (AMQPConnectionError, AMQPChannelError) as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {e}")
            return False

    def disconnect(self):
        """Close RabbitMQ connection."""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            self.logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            self.logger.error(f"Error disconnecting from RabbitMQ: {e}")

    def _enforce_rate_limit(self):
        """
        Enforce rate limiting by adding delay if necessary.
        Thread-safe method.
        """
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last_request = current_time - self._last_request_time
            
            if time_since_last_request < self.delay_between_requests:
                sleep_time = self.delay_between_requests - time_since_last_request
                time.sleep(sleep_time)
            
            self._last_request_time = time.time()

    def publish_cep(self, cep: str) -> bool:
        """
        Publish a CEP to the queue for processing.

        Args:
            cep: CEP to add to queue

        Returns:
            True if published successfully, False otherwise
        """
        if not self.channel or self.channel.is_closed:
            if not self.connect():
                return False

        try:
            message = json.dumps({'cep': cep})
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )
            )
            
            self.logger.debug(f"Published CEP {cep} to queue")
            return True
            
        except Exception as e:
            self.logger.error(f"Error publishing CEP {cep} to queue: {e}")
            return False

    def publish_multiple_ceps(self, ceps: list[str]) -> int:
        """
        Publish multiple CEPs to the queue.

        Args:
            ceps: List of CEPs to publish

        Returns:
            Number of CEPs successfully published
        """
        published_count = 0
        
        for cep in ceps:
            if self.publish_cep(cep):
                published_count += 1
        
        self.logger.info(f"Published {published_count}/{len(ceps)} CEPs to queue")
        return published_count

    def consume_ceps(
        self,
        callback: Callable[[str], Optional[Dict[str, Any]]],
        stop_after: Optional[int] = None
    ) -> int:
        """
        Consume CEPs from queue and process them with rate limiting.

        Args:
            callback: Function to process each CEP. Should accept CEP string
                     and return result dict or None
            stop_after: Stop after processing N CEPs (None = process all)

        Returns:
            Number of CEPs processed
        """
        if not self.channel or self.channel.is_closed:
            if not self.connect():
                return 0

        processed_count = 0
        self._stop_processing.clear()

        def process_message(ch, method, properties, body):
            nonlocal processed_count
            
            if self._stop_processing.is_set():
                ch.stop_consuming()
                return

            try:
                message = json.loads(body.decode('utf-8'))
                cep = message.get('cep')
                
                if not cep:
                    self.logger.warning("Received message without CEP")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                # Enforce rate limiting
                self._enforce_rate_limit()

                # Process CEP
                self.logger.info(f"Processing CEP {cep} ({processed_count + 1})")
                result = callback(cep)

                if result:
                    self.logger.debug(f"Successfully processed CEP {cep}")
                else:
                    self.logger.warning(f"Failed to process CEP {cep}")

                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                processed_count += 1

                # Check if we should stop
                if stop_after and processed_count >= stop_after:
                    self.logger.info(f"Reached limit of {stop_after} CEPs. Stopping consumption.")
                    ch.stop_consuming()

            except json.JSONDecodeError as e:
                self.logger.error(f"Error decoding message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=process_message
            )

            self.logger.info(f"Starting to consume from queue: {self.queue_name}")
            self.logger.info(f"Rate limit: {self.rate_limit_per_second} requests/second")
            
            self.channel.start_consuming()

        except KeyboardInterrupt:
            self.logger.info("Consumption stopped by user")
            self._stop_processing.set()
        except Exception as e:
            self.logger.error(f"Error during consumption: {e}")
        finally:
            if self.channel:
                self.channel.stop_consuming()

        self.logger.info(f"Processed {processed_count} CEPs")
        return processed_count

    def get_queue_size(self) -> int:
        """
        Get the number of messages in the queue.

        Returns:
            Number of messages in queue, or -1 if error
        """
        if not self.channel or self.channel.is_closed:
            if not self.connect():
                return -1

        try:
            method = self.channel.queue_declare(queue=self.queue_name, durable=True, passive=True)
            return method.method.message_count
        except Exception as e:
            self.logger.error(f"Error getting queue size: {e}")
            return -1

    def purge_queue(self) -> bool:
        """
        Purge all messages from the queue.

        Returns:
            True if purged successfully, False otherwise
        """
        if not self.channel or self.channel.is_closed:
            if not self.connect():
                return False

        try:
            self.channel.queue_purge(queue=self.queue_name)
            self.logger.info(f"Purged queue: {self.queue_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error purging queue: {e}")
            return False

