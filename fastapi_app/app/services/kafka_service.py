import logging
import json
from typing import Dict, Any, List, Optional, Callable

from confluent_kafka import Producer, Consumer, KafkaError

from app.core.config import settings

logger = logging.getLogger(__name__)

# Global Kafka producer
kafka_producer = None


def get_kafka_producer():
    """Get or create Kafka producer"""
    global kafka_producer
    
    if kafka_producer is not None:
        return kafka_producer
    
    try:
        # Create Producer instance
        kafka_producer = Producer({
            'bootstrap.servers': settings.KAFKA_BROKER
        })
        logger.info("Successfully initialized Kafka producer")
        return kafka_producer
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {str(e)}")
        raise


def delivery_report(err, msg):
    """Callback function for message delivery results"""
    if err is not None:
        logger.error(f"Failed sending Kafka message: {err}")
    else:
        logger.info(f"Successfully sent Kafka message to topic: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def send_message(topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
    """Send a message to a Kafka topic"""
    try:
        producer = get_kafka_producer()
        
        # Convert message to JSON
        message_json = json.dumps(message).encode('utf-8')
        
        # Send message
        producer.produce(
            topic=topic,
            key=key.encode('utf-8') if key else None,
            value=message_json,
            callback=delivery_report
        )
        
        # Wait for any outstanding messages to be delivered
        producer.flush()
        
        return True
    except Exception as e:
        logger.error(f"Failed to send message to topic {topic}: {str(e)}")
        return False


def create_consumer(topics: List[str], group_id: Optional[str] = None) -> Consumer:
    """Create a Kafka consumer"""
    try:
        # Configure consumer
        conf = {
            'bootstrap.servers': settings.KAFKA_BROKER,
            'group.id': group_id or settings.KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
        
        # Create Consumer instance
        consumer = Consumer(conf)
        
        # Subscribe to topics
        consumer.subscribe(topics)
        
        logger.info(f"Created Kafka consumer for topics: {topics}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {str(e)}")
        raise


def consume_messages(consumer: Consumer, handler: Callable[[Dict[str, Any]], None], timeout: float = 1.0, max_messages: int = 100) -> int:
    """Consume messages from Kafka topics"""
    try:
        messages_processed = 0
        
        for _ in range(max_messages):
            msg = consumer.poll(timeout)
            
            if msg is None:
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                else:
                    logger.error(f"Error consuming message: {msg.error()}")
                continue
            
            try:
                # Parse message
                message_value = json.loads(msg.value().decode('utf-8'))
                
                # Process message
                handler(message_value)
                
                messages_processed += 1
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
        
        return messages_processed
    except Exception as e:
        logger.error(f"Error consuming messages: {str(e)}")
        return 0 