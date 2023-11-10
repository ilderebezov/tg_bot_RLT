import asyncio
import json
import os
from functools import partial

from telebot.async_telebot import AsyncTeleBot

from src.data_aggregation import aggregation
from src.mongodb_drv import get_data_from_db
import logging

bot = None
try:
    bot = AsyncTeleBot(os.environ['BOT_TOKEN'])
except KeyError as error_bot_token:
    print(f"Add BOT_TOKEN variable to environment")
    logging.error(f"BOT_TOKEN error: {error_bot_token}", exc_info=True)
except Exception as error_bot_init:
    logging.error(f"Bot init error: {error_bot_init}", exc_info=True)
    print(f"{error_bot_init=}")


@bot.message_handler(commands=['help', 'start'])
async def send_welcome(message):
    """
    answer on 'help', 'start' message
    :param message:
    :return:
    """
    await bot.reply_to(message, """\
    Агрегация статистических данных о зарплатах 
    сотрудников компании по временным промежуткам.
    Типы агрегации могут быть следующие: hour, day, week, month. 
    То есть группировка всех данных за час, день, неделю, месяц.
    \
""")


@bot.message_handler(func=lambda message: True)
async def bot_message(message):
    """
    main end-point to work with user message
    :param message:
    :return:
    """
    try:
        message_text = message.text
        message_data_dict = json.loads(message_text)
        loop = asyncio.get_event_loop()
        df_from_find_db = await loop.run_in_executor(None,
                                                     partial(get_data_from_db,
                                                             dt_iso_from=message_data_dict.get("dt_from"),
                                                             dt_iso_upto=message_data_dict.get("dt_upto")))

        dataset, label = await loop.run_in_executor(None,
                                                    partial(aggregation,
                                                            df_data_in=df_from_find_db,
                                                            group_type=message_data_dict.get("group_type"),
                                                            day_from=message_data_dict.get("dt_from"),
                                                            day_upto=message_data_dict.get("dt_upto")))
        out = {"dataset": dataset,
               "labels": label}
    except Exception as error_bot_message:
        out = f"Request ERROR: {error_bot_message}. Update your request and try again."
        logging.error(f"Bot_message error: {error_bot_message}", exc_info=True)
    await bot.send_message(chat_id=message.chat.id, text=f"{out}")
