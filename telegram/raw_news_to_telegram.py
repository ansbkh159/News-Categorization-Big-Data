import json
from telethon import TelegramClient
from kafka import KafkaProducer
from os import environ
from dotenv import load_dotenv
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

class TelegramNewsSender:
    def __init__(self, api_id, api_hash, session_name, index, json_file_path='/var/www/html/big-data/news-dataset.json', kafka_bootstrap_servers=None):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.channel_username = -1002326731921
        self.json_file_path = json_file_path
        self.index = index 
        self.kafka_producer = None
        if kafka_bootstrap_servers:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=kafka_bootstrap_servers,
                    value_serializer=lambda v: v.encode("utf-8") if v else None
                )
                logger.info("Kafka producer initialized.")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {str(e)}")
                self.kafka_producer = None

    def read_dataset_content(self):
        """Read JSON dataset (array or JSON Lines) and filter out financial articles."""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                # Try loading as a JSON array
                try:
                    data = json.load(file)
                    if not isinstance(data, list):
                        logger.error(f"JSON file must contain a list of objects, got {type(data)}")
                        sys.exit(1)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to load as JSON array: {str(e)}. Trying JSON Lines format...")
                    file.seek(0)  # Reset file pointer
                    data = []
                    for line_num, line in enumerate(file, 1):
                        line = line.strip()
                        if line:
                            try:
                                article = json.loads(line)
                                data.append(article)
                            except json.JSONDecodeError as je:
                                logger.warning(f"Skipping invalid JSON at line {line_num}: {str(je)}")

                for i, article in enumerate(data):
                    if i < self.index:
                        continue
                    headline = article.get('headline', '')
                    authors = article.get('authors', '')
                    short_description = article.get('short_description', '')
                    if isinstance(authors, list):
                        authors = ', '.join(authors) if authors else 'Unknown'
                    else:
                        authors = authors or 'Unknown'
                    concatenated_text = (
                        f"Headline: {headline}, "
                        f"Authors: {authors}, "
                        f"Description: {short_description}"
                    )
                    yield concatenated_text
        except FileNotFoundError:
            logger.error(f"JSON file not found: {self.json_file_path}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading JSON dataset: {str(e)}")
            sys.exit(1)

    async def send_message_to_channel(self, message_text):
        """Send a message to the Telegram channel."""
        try:
            async with TelegramClient(self.session_name, self.api_id, self.api_hash) as client:
                await client.send_message(entity=self.channel_username, message=message_text)
                logger.info("News article sent to Telegram.")
        except Exception as e:
            logger.error(f"Failed to send message to Telegram: {str(e)}")

    async def send_message_to_kafka(self, message_text):
        """Send a message to Kafka general-news-2 topic."""
        if self.kafka_producer:
            try:
                self.kafka_producer.send("general-news-2", value=message_text)
                self.kafka_producer.flush()
                logger.info("News article sent to Kafka topic 'general-news-2'.")
            except Exception as e:
                logger.error(f"Failed to send message to Kafka: {str(e)}")

    async def send_dataset_to_channel(self):
        """Process and send dataset articles to Telegram and Kafka."""
        for message_text in self.read_dataset_content():
            await self.send_message_to_channel(message_text)
            # await self.send_message_to_kafka(message_text)

if __name__ == "__main__":
    import asyncio
    try:
        api_id = environ["API_ID"]
        api_hash = environ["API_HASH"]
        kafka_servers = environ["BOOTSTRAP_SERVERS"] if "BOOTSTRAP_SERVERS" in environ else "localhost:9092"  # Kafka bootstrap servers
        sender = TelegramNewsSender(
            api_id=api_id,
            api_hash=api_hash,
            session_name="anas",
            index=20,
            json_file_path="/var/www/html/big-data/news-dataset.json",
            kafka_bootstrap_servers=kafka_servers
        )
        asyncio.run(sender.send_dataset_to_channel())
    except KeyError as e:
        logger.error(f"Environment variable missing: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running sender: {str(e)}")
        sys.exit(1)