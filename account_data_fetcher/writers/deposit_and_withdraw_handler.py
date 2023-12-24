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
    """
    Class representing Deposit and Withdrawal information.

    Attributes:
        date (str): Date of the transaction.
        from_exchange (Optional[str]): The exchange the money was withdrawn from.
        to_exchange (Optional[str]): The exchange the money was deposited to.
        amount (float): The transaction amount.
        comment (Optional[str]): Optional comment.
    """
    date: str
    from_exchange: Optional[str]
    to_exchange: Optional[str]
    amount: float
    comment: Optional[str]

    def write_dataclass_to_csv(self, path: str, file_name: str= "deposits_and_withdraws.csv") -> None:
        """
        Writes the dataclass to a CSV file.
        
        Args:
            path (str): The CSV file path to write to.
        """
        dict_obj = asdict(self)
        file_exists = self.is_file_in_folder(path, file_name)
        path += "/" + file_name
        with open(path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=dict_obj.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(dict_obj)

    def is_file_in_folder(self, path:str, file_name: str) -> bool:
        """
        Checks if a file exists in the given folder.
        
        Args:
            path (str): Folder path.
            file_name (str): File name to check.
        
        Returns:
            bool: True if file exists, otherwise False.
        """
        onlyfiles = []
        onlyfiles += [f for f in listdir(path) if isfile(join(path, f)) and f != ".DS_Store" and getsize(join(path, f)) > 0]
        return file_name in onlyfiles
    
#TODO: For the bravest, automate the below by listening to withdraw/deposits for each exchanges and updating the database as needed.
class depositAndWithdrawHandler:
    """
    Class to handle deposit and withdrawal transactions.

    Attributes:
        __SUPPORTED_EXCHANGE (set): Set of supported exchanges.
        data (DepositAndWithdraw): An instance of DepositAndWithdraw dataclass.
    """
    __SUPPORTED_EXCHANGE: set = ("BINANCE", "BYBIT", "DYDX", "ETHEREUM", "IB", "TRADESTATION", "KRAKEN", "ONCHAIN")    
    
    def __init__(self) -> None:
        """Initializes the depositAndWithdrawHandler object and prompts for transaction info."""
        print(f"We will now prompt you some information so that your deposit and withdraw can be handled. \n Please note the list of supported exchange is the following {self.__SUPPORTED_EXCHANGE}")
        self.data: DepositAndWithdraw = self.__get_data()
        self.data.write_dataclass_to_csv(self.get_base_path() + "/account_data_fetcher/csv_db/", "deposits_and_withdraws.csv")

    @staticmethod
    def get_base_path():
        """
        Returns the base path.
        
        Returns:
            str: The base directory path.
        """
        current_directory = os.path.dirname(__file__)
        return os.path.abspath(os.path.join(current_directory, '..', '..'))
        
    def __get_data(self) -> DepositAndWithdraw:
        """
        Collects data from user input and creates a DepositAndWithdraw object.
        
        Returns:
            DepositAndWithdraw: A filled DepositAndWithdraw dataclass.
        """
        return DepositAndWithdraw(
            date=self.get_date(),
            from_exchange=self.get_from_exchange(),
            to_exchange=self.get_to_exchange(),
            amount=self.get_user_amount(),
            comment=self.get_comment()
        )

    def get_date(self) -> datetime:
        """
        Gets the transaction date from user input.

        Returns:
            datetime: The transaction date.
        """
        date: Optional[str] = input("If the transaction is not from today, specify the date in dd/mm/yyy format, otherwise press enter and it'll get generated automatically \n")
        if not date:
            date = datetime.utcnow().strftime('%d/%m/%Y')
        else:
            if not self.is_valid_date(date, "%d/%m/%Y"):
                raise ValueError("Please provide a valid date")
        return date
    
    def is_valid_date(self, date_str: str, format: str) -> bool:
        """
        Validates the provided date string.
        
        Args:
            date_str (str): Date string to validate.
            format (str): The format to use for date validation.
        
        Returns:
            bool: True if valid, otherwise False.
        """
        try:
            datetime.strptime(date_str, format)
            return True
        except ValueError:
            return False 

    def get_from_exchange(self) -> Optional[str]:
        """
        Gets the 'from_exchange' information from user input.
        
        Returns:
            Optional[str]: The name of the exchange or None.
        """
        from_exchange: Optional[str] = input("Please insert from which exchange you withdrew. If None (money inflow), press enter \n")
        if not from_exchange:
            return None
        else:
            if from_exchange.upper() not in self.__SUPPORTED_EXCHANGE:
                raise NotImplementedError(f"this exchange not in allow list: {self.__SUPPORTED_EXCHANGE}")
            else:
                return from_exchange.upper()
    
    def get_to_exchange(self) -> Optional[str]:
        """
        Gets the 'to_exchange' information from user input.
        
        Returns:
            Optional[str]: The name of the exchange or None.
        """
        to_exchange: Optional[str] = input("Please insert from to exchange you deposited. If None (money outflow), press enter \n")
        if not to_exchange:
            return None
        else:
            if to_exchange.upper() not in self.__SUPPORTED_EXCHANGE:
                raise NotImplementedError(f"this exchange not in allow list: {self.__SUPPORTED_EXCHANGE}")
            else:
                return to_exchange.upper()

    def get_user_amount(self) -> float:
        """
        Gets the transaction amount from the user.

        Returns:
            float: The transaction amount.
        
        Notes:
            Withdrawals = negatif, deposits = positifs
        """
        amount: str = input("Please insert the amount you deposited or withdrawn. If withdrawn, include a - sign \n")
        return float(amount)

    def get_comment(self) -> str:
        """
        Gets any additional comments on the transaction from the user.
        
        Returns:
            str: The comment.
        """
        comment: str = input("If wanted you can add a comment on that movement \n")
        return comment
    
if __name__ == "__main__":
    depositAndWithdrawHandler()
