import logging
import os

import telegram

from infrastructure.api_secret_getter import ApiMetaData, ApiSecretGetter


class telegramHandler:
    __PROGRAM = "Telegram"

    def __init__(self, path: str, password: str) -> None:
        secrets_path = self._resolve_secrets_path(path)
        secrets: dict[str, ApiMetaData] = ApiSecretGetter.get_api_meta_data(secrets_path, password)
        self.bot: telegram.Bot = telegram.Bot(secrets[self.__PROGRAM].key)
        self.channel_id: str = "-" + secrets[self.__PROGRAM].other_fields["Channel_id"]

    @staticmethod
    def _resolve_secrets_path(path: str) -> str:
        if os.path.isfile(os.path.join(path, ".pk.txt")):
            return path
        base = path
        if os.path.basename(path) in {"monitor", "utilities", "infrastructure", "account_data_fetcher"}:
            base = os.path.abspath(os.path.join(path, ".."))
        return os.path.join(base, "account_data_fetcher", "secrets")

    async def send_photo_to_telegram(self, file_path: str) -> None:
        return await self.bot.send_photo(self.channel_id, photo=open(file_path, "rb"))

    async def send_text_to_telegram(self, text: str, parse_mode: str | None = None) -> None:
        if not parse_mode:
            return await self.bot.send_message(
                self.channel_id,
                text=text,
            )

        else:
            return await self.bot.send_message(self.channel_id, text=text, parse_mode=parse_mode)


if __name__ == "__main__":
    from getpass import getpass

    log_filename = os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format="%(levelname)s:%(asctime)s:%(message)s", filename=log_filename)
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    print(current_path)
    executor = telegramHandler(current_path, pwd, logger)
