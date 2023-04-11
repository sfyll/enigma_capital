import logging
import os
from typing import Optional

import telegram

from utilities.account_data_fetcher_base import accountFetcherBase

class telegramHandler(accountFetcherBase):
    __PROGRAM = "Telegram"
    def __init__(self, path: str, password: str) -> None:
        super().__init__(path, password)
        self.bot: telegram.Bot = telegram.Bot(self.api_meta_data[self.__PROGRAM].key)
        self.channel_id: str = "-" +  self.api_meta_data[self.__PROGRAM].other_fields["Channel_id"]

    async def send_photo_to_telegram(self, file_path: str) -> None:
        return await self.bot.send_photo(
                self.channel_id,
                photo=open(file_path, "rb")
            )

    async def send_text_to_telegram(self, text: str, parse_mode: Optional[str] = None) -> None:
        if not parse_mode:
            return await self.bot.send_message(
                    self.channel_id,
                    text=text,
            )
        
        else:
             return await self.bot.send_message(
                    self.channel_id,
                    text=text,
                    parse_mode=parse_mode     
            )


if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    print(current_path)
    executor = telegramHandler(current_path, pwd, logger)

