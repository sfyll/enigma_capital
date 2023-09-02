# UnNamed-Fund - The Modern Intelligent Investor 

- Monitor your positions and balances across crypto and traditional exchanges;
- Save the above to build a track-record and run statistical analysis, with google sheets being used as back-up;
- Quantify your thesis, create dashboards and have these sent to the Telegram channel of your choice.

All in all, use this repo to invest smarter and better, less is more!

## Supported Exchanges:

- Binance;
- Bybit;
- DYDX; 
- Ethereum; 
- Interactive Brokers; 
- TradeStation;

Please note this is for basic usage. I quickly built up this repo and so it's not complete by any means, for example:

- If latency matters, don't use it;
- If you're planning to fetch your balances across 30 ERC20s, don't use it;

My goal with this code was to save time going forward, not to make money. Nonetheless, any contribution is always appreciated as they are quite a few low hanging fruits!

PS: You'll find what I meant by dashboard by being able to run the code in `/thesis_monitoring/hkd_peg_breaking/` !

## Installation

1. Install virtualenv: `sudo pip install virtualenv`
1. Clone the repo;
2. Navigate to the base repo directory and run `virtualenv env`
3. Activate your virtual environment: `source env/bin/activate`
4. Install the requirements: `pip install -r requirements.txt`
5. Download rust for cryptography lib: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
6. Create a private key using `encryptor.py` in `/utilities` (USE A PASSWORD WHEN DOING SO), since the project looks for private keys in each directory, you would for example run `python3 ../utilities/encryptor.py` while setting-up your account-data-fetcher within that directory, and so on for each directory.
7. Using `encryptor.py`, start writting your first API keys using `write_api_key_enc_to_file` and then add as many as you wish with `add_keys_to_encrypted_file`, the pattern being the following:
```
    key_information = {
        "Key":"",
        "Secret":"",
        'Other_fields': {}
    }
```
8. Finally, don't forget to also create an encrypted `.gsheet.txt` file by grabbing a json from google (cf https://medium.com/craftsmenltd/from-csv-to-google-sheet-using-python-ef097cb014f9)
9. To send thesis to yourself, you'll need to follow the steps 6 to 7 within the `/thesis_monitoring` directory
10. Simply run the scripts (you might have to do `chmod+x scriptname.sh` beforehand), and you're good to go! To run these scripts in the background, wait until you have inputted your password, and just press `CTRL+z` and then run the command `bg`.


For those that want higher frequency than daily data, they'll need to use the IBC gateway. Please note, if you have a raspberry pi, it's currently impossible to use it!

IBC requirements:

Install offline tws : https://www.interactivebrokers.com/en/trading/tws-offline-installers.php

Download IBC: https://github.com/IbcAlpha/IBC/releases  

Download JAVA to run IBC if `java -version` returns nothing: `sudo apt install default-jdk`

On Unix:

Unpack the ZIP file using a command similar to this:
`sudo unzip ~/Downloads/IBCLinux-3.6.0.zip -d /opt/ibc`
Now make sure all the script files are executable:
```
cd /opt/ibc
sudo chmod o+x *.sh */*.sh
```
Check that the correct major version number for TWS is set in the shell script files in the IBC installation folder: these files are `StartTWS.bat` and `StartGateway.bat` on Windows, `twsstart.sh` and `gatewaystart.sh` on Unix, `twsstartmacos.sh` and `gatewaystartmacos.sh` on macOS.

To find the TWS major version number, first run TWS or the Gateway manually using the IBKR-provided icon, then click Help > About Trader Workstation or Help > About IB Gateway. In the displayed information you'll see a line similar to this:

   Build 10.19.1f, Oct 28, 2022 3:03:08 PM
  
For Windows and Linux, the major version number is 1019 (ie ignore the period after the first part of the version number).

For macOS, the major version number is 10.19. (Note that this is different from the equivalent Windows and Linux settings because the macOS installer includes the period in the install folder name).

Now open the script files with a text editor and ensure that the TWS_MAJOR_VRSN variable is set correctly.

Make the project root an environment variable equal to enigma, for ex with zsh shells: echo 'export ENIGMA=~/Documents/dev/enigma_capital ' >> ~/.zshenv

Keep in mind you'll have to make changes to ibflex as library is not updated, from lines 263 in their Types document, use diff checker and their main repo to make this easier!

All set and ready to go !

 
 ## TODOs
- Be able to subscribe to different topics, so that the data_aggregator can scale easily to more usecases ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/data_aggregator/data_aggregator.py#L286))
- Handle failure. What if an exchange stop publishing and we just loop forever? ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/data_aggregator/data_aggregator.py#L291))
- Make the function more modular, the data_aggregator should be agnostic to what data is being aggregated as that should be abstracted away. ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/data_aggregator/data_aggregator.py#L292))
- Properly parse the object below so that the inner dataclass can be read as a dataclass, and not accessed as a dict ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/data_aggregator/data_aggregator.py#L318))
- For now, we only enforce two methods implementation, namely fetch_balance and fetch_positions. As such, process_request is quite statically defined as well. How could we untangle both so that we can define more abstract methods and have the process_request understands what to fetch dynamically. ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/exchanges/exchange_base.py#L12))
- Batch calls via multicall contracts + use helios lightweight client (need to fix eth_call loops, broken atm) ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/exchanges/ethereum/data_fetcher.py#L42))
- Fetch positions ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/exchanges/ib_async/data_fetcher.py#L49))
- Fix arbitrary ConnectionResetError bug requests.exceptions.ConnectionError: ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer')) ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/exchanges/dydx/data_fetcher.py#L65))
- Handle subscription on per topic basis ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/writers/writer_base.py#L100))
- For the bravest, automate the below by listening to withdraw/deposits for each exchanges and updating the database as needed. ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/writers/deposit_and_withdraw_handler.py#L58))
- Process request is the only entry-point at the factory level. This could be made more generic to accomodate for other entry-points and inputs as the application scales. ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/account_data_fetcher/launcher/process_factory_base.py#L43))
- Take a look at account_data_fetcher, how could you refactor the entire monitor component? ([source](https://github.com/SFYLL/enigma_capital/blob/make_it_better/monitor/runner.py#L20))