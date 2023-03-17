import csv
import dataclasses
from dataclasses import asdict
from datetime import datetime
import os
from os import listdir
from os.path import isfile, join, getsize
from typing import Optional

@dataclasses.dataclass(init=True, eq=True, repr=True)
class DepositAndWithdraw:
    date: str
    from_exchange: Optional[str]
    to_exchange: Optional[str]
    amount: float
    comment: Optional[str]

    def write_dataclass_to_csv(self, file_name:str) -> None:
        dict_obj = asdict(self)
        path = os.path.realpath(os.path.dirname(__file__))
        file_exists = self.is_file_in_folder(path, file_name)
        file_name: str = path + "/" + file_name
        with open(file_name, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=dict_obj.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(dict_obj)

    def is_file_in_folder(self, path:str, file_name: str) -> bool:
        onlyfiles = []
        onlyfiles += [f for f in listdir(path) if isfile(join(path, f)) and f != ".DS_Store" and getsize(join(path, f)) > 0]
        return file_name in onlyfiles
    
class depositAndWithdrawHandler:
    __SUPPORTED_EXCHANGE: set = ("BINANCE", "BYBIT", "DYDX", "ETHEREUM", "IB", "TRADESTATION", "KRAKEN")    
    def __init__(self) -> None:
        print(f"We will now prompt you some information so that your deposit and withdraw can be handled. \n Please note the list of supported exchange is the following {self.__SUPPORTED_EXCHANGE}")
        self.data: DepositAndWithdraw = self.__get_data()
        self.data.write_dataclass_to_csv("deposits_and_withdraws.csv")
        
    def __get_data(self) -> DepositAndWithdraw:
        return DepositAndWithdraw(
            date=self.get_date(),
            from_exchange=self.get_from_exchange(),
            to_exchange=self.get_to_exchange(),
            amount=self.get_user_amount(),
            comment=self.get_comment()
        )

    def get_date(self) -> datetime:
        date: Optional[str] = input("If the transaction is not from today, specify the date in dd/mm/yyy format, otherwise press enter and it'll get generated automatically \n")
        if not date:
            date = datetime.utcnow().strftime('%d/%m/%Y')
        return date

    def get_from_exchange(self) -> Optional[str]:
        from_exchange: Optional[str] = input("Please insert from which exchange you withdrew. If None (money inflow), press enter \n")
        if not from_exchange:
            return None
        else:
            if from_exchange.upper() not in self.__SUPPORTED_EXCHANGE:
                raise NotImplementedError(f"this exchange not in allow list: {self.__SUPPORTED_EXCHANGE}")
            else:
                return from_exchange.upper()
    
    def get_to_exchange(self) -> Optional[str]:
        to_exchange: Optional[str] = input("Please insert from to exchange you deposited. If None (money outflow), press enter \n")
        if not to_exchange:
            return None
        else:
            if to_exchange.upper() not in self.__SUPPORTED_EXCHANGE:
                raise NotImplementedError(f"this exchange not in allow list: {self.__SUPPORTED_EXCHANGE}")
            else:
                return to_exchange.upper()

    def get_user_amount(self) -> float:
        """Withdrawals = negatif, deposits = positifs"""
        amount: str = input("Please insert the amount you deposited or withdrawn. If withdrawn, include a - sign \n")
        return float(amount)

    def get_comment(self) -> str:
        comment: str = input("If wanted you can add a comment on that movement \n")
        return comment
    
if __name__ == "__main__":
    depositAndWithdrawHandler()