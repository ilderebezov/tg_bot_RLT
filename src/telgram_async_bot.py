import os

from telebot.async_telebot import AsyncTeleBot

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
