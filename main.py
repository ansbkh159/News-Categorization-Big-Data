from telegram.news_sender_per_telegram_account import TelegramNewsSender
from telegram.kafka_sender import NewsReader
from os import environ
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_id = environ["API_ID"]
api_hash = environ["API_HASH"]

accounts = {
    "anas": {
        "api_id": api_id,
        "api_hash": api_hash,
        "session_name": "anas",
        "index": 20,
    },
}

async def send_news_to_telegram_channel(api_id, api_hash, session_name, index):
    try:
        sender = TelegramNewsSender(api_id, api_hash, session_name, index)
        await sender.send_dataset_to_channel()
        logger.info(f"Successfully sent news to Telegram for session {session_name}")
    except Exception as e:
        logger.error(f"Error sending to Telegram for session {session_name}: {e}")

async def send_news_to_kafka(api_id, api_hash, session_name):
    try:
        sender = NewsReader(api_id, api_hash, session_name)
        await sender.run()
        logger.info(f"Successfully sent news to Kafka.")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")

async def process_account(account_details):
    telegram_session_name = account_details["session_name"] + "_telegram"
    kafka_session_name = account_details["session_name"] + "_kafka"
    
    await asyncio.gather(
        send_news_to_telegram_channel(
            account_details["api_id"],
            account_details["api_hash"],
            telegram_session_name,
            account_details["index"],
        ),
        send_news_to_kafka(
            account_details["api_id"],
            account_details["api_hash"],
            kafka_session_name,
        ),
    )

async def main():
    tasks = [process_account(account_details) for account_details in accounts.values()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Error: {e}")
