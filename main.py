import asyncio
import logger_
import logging
from src.telgram_async_bot import bot


def init():
    """
    init bot
    :return: None
    """
    try:
        asyncio.run(bot.polling())
    except Exception as error_init:
        logging.error(f"Init error: {error_init}", exc_info=True)


if __name__ == '__main__':
    init()
