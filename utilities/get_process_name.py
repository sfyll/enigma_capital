import re

def get_process_name(log_file: str) -> str:
    """
    Pattern example : ' ~/log/account_data_fetcher.log', will return balance_fetcher
    """
    return re.split(r"[/.]", log_file)[-2]

if __name__ == "__main__":
    print(get_process_name(" ~/log/account_data_fetcher.log"))