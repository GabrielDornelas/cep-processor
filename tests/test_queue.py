"""
Unit tests for queue manager module
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import pika

from src.queue.queue_manager import QueueManager


@pytest.fixture
def mock_env_vars():
    """Fixture to mock environment variables for queue tests"""
    with patch.dict('os.environ', {
        'RABBITMQ_URL': 'amqp://guest:guest@localhost:5672/',
        'RATE_LIMIT_PER_SECOND': '5.0'
    }):
        yield


class TestQueueManager:
    """Test cases for QueueManager class"""

    def test_init(self):
        """Test QueueManager initialization"""
        manager = QueueManager(
            rabbitmq_url="amqp://guest:guest@localhost:5672/",
            queue_name="test_queue",
            rate_limit_per_second=5.0
        )
        
        assert manager.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert manager.queue_name == "test_queue"
        assert manager.rate_limit_per_second == 5.0
        assert manager.delay_between_requests == 0.2  # 1.0 / 5.0

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_connect_success(self, mock_connection_class, mock_env_vars):
        """Test successful RabbitMQ connection"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        manager = QueueManager()
        result = manager.connect()
        
        assert result is True
        assert manager.connection == mock_connection
        assert manager.channel == mock_channel
        mock_channel.basic_qos.assert_called_once()
        mock_channel.queue_declare.assert_called_once()

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_connect_failure(self, mock_connection_class, mock_env_vars):
        """Test failed RabbitMQ connection"""
        mock_connection_class.side_effect = pika.exceptions.AMQPConnectionError("Connection failed")
        
        manager = QueueManager()
        result = manager.connect()
        
        assert result is False

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_disconnect(self, mock_connection_class, mock_env_vars):
        """Test disconnecting from RabbitMQ"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        manager = QueueManager()
        manager.connect()
        manager.disconnect()
        
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_publish_cep(self, mock_connection_class, mock_env_vars):
        """Test publishing CEP to queue"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        manager = QueueManager()
        manager.connect()
        result = manager.publish_cep("01310100")
        
        assert result is True
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        assert call_args[1]['routing_key'] == manager.queue_name
        message_body = json.loads(call_args[1]['body'])
        assert message_body['cep'] == "01310100"

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_publish_multiple_ceps(self, mock_connection_class, mock_env_vars):
        """Test publishing multiple CEPs"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        manager = QueueManager()
        manager.connect()
        
        ceps = ["01310100", "01310101", "01310102"]
        count = manager.publish_multiple_ceps(ceps)
        
        assert count == 3
        assert mock_channel.basic_publish.call_count == 3

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_get_queue_size(self, mock_connection_class, mock_env_vars):
        """Test getting queue size"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_method = Mock()
        mock_method.method.message_count = 42
        mock_channel.queue_declare.return_value = mock_method
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        manager = QueueManager()
        manager.connect()
        size = manager.get_queue_size()
        
        assert size == 42

    @patch('src.queue.queue_manager.pika.BlockingConnection')
    def test_purge_queue(self, mock_connection_class, mock_env_vars):
        """Test purging queue"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        manager = QueueManager()
        manager.connect()
        result = manager.purge_queue()
        
        assert result is True
        mock_channel.queue_purge.assert_called_once_with(queue=manager.queue_name)

    def test_enforce_rate_limit(self, mock_env_vars):
        """Test rate limiting enforcement"""
        import time
        
        manager = QueueManager(rate_limit_per_second=10.0)  # 10 req/s = 0.1s delay
        
        # First call should not delay
        start = time.time()
        manager._enforce_rate_limit()
        first_call_time = time.time() - start
        assert first_call_time < 0.05  # Should be very fast
        
        # Second call should delay
        start = time.time()
        manager._enforce_rate_limit()
        second_call_time = time.time() - start
        assert second_call_time >= 0.08  # Should have delay (~0.1s)

