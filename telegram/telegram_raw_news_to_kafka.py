from telethon import TelegramClient, events
from os import environ
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer

load_dotenv()

class NewsReader:
    def __init__(self, api_id, api_hash, session_name):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.channel_username = -1002326731921
        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        self.producer = None

    async def start_producer(self):
        self.producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
        await self.producer.start()

    async def stop_producer(self):
        if self.producer:
            await self.producer.stop()

    async def send_to_kafka(self, message, topic='general-news-2'):
        try:
            await self.producer.send_and_wait(topic, message.encode('utf-8'))
            print(f"Message sent to Kafka: {message[:10]}...")
        except Exception as e:
            print(f"Error sending to Kafka: {e}")

    async def setup_event_handler(self):
        @self.client.on(events.NewMessage(chats=self.channel_username))
        async def handler(event):
            if event.message.text:
                await self.send_to_kafka(event.message.text)

    async def run(self):
        await self.start_producer()
        try:
            await self.setup_event_handler()
            print(f"Listening for new messages in channel {self.channel_username}...")
            await self.client.start()
            await self.client.run_until_disconnected()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await self.stop_producer()

if __name__ == "__main__":
    import asyncio
    api_id = environ["API_ID"]
    api_hash = environ["API_HASH"]
    news_reader = NewsReader(api_id, api_hash, session_name="anas")
    asyncio.run(news_reader.run())