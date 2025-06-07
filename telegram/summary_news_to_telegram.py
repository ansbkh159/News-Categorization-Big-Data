import json
import asyncio
import logging
from os import environ
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from telethon import TelegramClient
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

class KafkaToTelegramSender:
    def __init__(self, api_id, api_hash, session_name, kafka_topic='news-summary', kafka_bootstrap_servers='localhost:9092'):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        
        self.channel_map = {
            "political": -1002543571391,
            "environmental": -1002527026044,
            "health": -1002598764116,
            "technology": -1002695179664,
            "arts": -1002642900359,
            "sports": -1002389263475,
            "social": -1002565151711
        }
        self.categories = ["environmental", "health", "technology", "political", "arts", "sports", "social"]

    async def send_to_telegram(self, message_text, channel_id):
        """Send a message to the specified Telegram channel."""
        try:
            async with self.client:
                await self.client.send_message(entity=channel_id, message=message_text)
                logger.info(f"Message sent to Telegram channel {channel_id}: {message_text[:30]}...")
        except Exception as e:
            logger.error(f"Failed to send message to Telegram channel {channel_id}: {str(e)}")

    async def consume_kafka(self):
        """Periodically check Kafka for messages every 5 seconds and send to appropriate Telegram channel."""
        consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id="news_summary_group",
            auto_offset_reset="earliest"
        )
        try:
            await consumer.start()
            logger.info(f"Started consuming from Kafka topic: {self.kafka_topic}")
            while True:
                try:
                    messages = await consumer.getmany(timeout_ms=1000)
                    for topic_partition, msgs in messages.items():
                        for msg in msgs:
                            try:
                                message = msg.value.decode('utf-8')
                                data = json.loads(message)
                                category = data.get('col2', '').lower()
                                message_text = data.get('col1', '')

                                if not message_text:
                                    logger.warning("Message has no content in 'col1'. Skipping...")
                                    continue

                                if category not in self.categories:
                                    logger.warning(f"Unknown category '{category}'. Skipping...")
                                    continue

                                channel_id = self.channel_map.get(category)
                                if not channel_id:
                                    logger.error(f"No channel ID mapped for category '{category}'. Skipping...")
                                    continue

                                await self.send_to_telegram(message_text, channel_id)

                            except json.JSONDecodeError as je:
                                logger.error(f"Invalid JSON in Kafka message: {str(je)}")
                            except Exception as e:
                                logger.error(f"Error processing Kafka message: {str(e)}")
                    
                    await asyncio.sleep(5)

                except Exception as e:
                    logger.error(f"Error during Kafka polling: {str(e)}")
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Error in Kafka consumer: {str(e)}")
        finally:
            await consumer.stop()
            logger.info("Kafka consumer stopped.")

    async def run(self):
        """Start the Kafka consumer and Telegram client."""
        try:
            await self.client.start()
            logger.info("Telegram client started.")
            await self.consume_kafka()
        except Exception as e:
            logger.error(f"Error running KafkaToTelegramSender: {str(e)}")
        finally:
            await self.client.disconnect()
            logger.info("Telegram client disconnected.")

if __name__ == "__main__":
    try:
        api_id = environ["API_ID"]
        api_hash = environ["API_HASH"]
        kafka_servers = environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
        sender = KafkaToTelegramSender(
            api_id=api_id,
            api_hash=api_hash,
            session_name="anas",
            kafka_topic="news-summary",
            kafka_bootstrap_servers=kafka_servers
        )
        asyncio.run(sender.run())
    except KeyError as e:
        logger.error(f"Environment variable missing: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error starting script: {str(e)}")
        sys.exit(1)