from asyncio import current_task
from getpass import getpass
import json
import os
from tempfile import mkdtemp
import warnings

import pgpy
from pgpy.constants import (
PubKeyAlgorithm, KeyFlags, HashAlgorithm,
SymmetricKeyAlgorithm, CompressionAlgorithm)
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning) 


def pgpy_encrypt(key, data):
    message = pgpy.PGPMessage.new(data)
    enc_message = key.pubkey.encrypt(message)
    return str(enc_message)

def pgpy_decrypt(key, enc_data):
    message = pgpy.PGPMessage.from_blob(enc_data)
    return str(key.decrypt(message).message)

def pgpy_create_key(name: str, comment:str, email: str, pwd: str = None):
    # we can start by generating a primary key. For this example, we'll use RSA, but it could be DSA or ECDSA as well
    key = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)
    # we now have some key material, but our new key doesn't have a user ID yet, and therefore is not yet usable!
    #example below: name could be your own name, comment anything like "API keys"
    uid = pgpy.PGPUID.new(name, comment=comment, email=email)
    # now we must add the new user id to the key. We'll need to specify all of our preferences at this point
    # because PGPy doesn't have any built-in key preference defaults at this time
    # this example is similar to GnuPG 2.1.x defaults, with no expiration or preferred keyserver
    key.add_uid(uid, usage={KeyFlags.Sign, KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage},
    hashes=[HashAlgorithm.SHA256, HashAlgorithm.SHA384, HashAlgorithm.SHA512, HashAlgorithm.SHA224],
    ciphers=[SymmetricKeyAlgorithm.AES256, SymmetricKeyAlgorithm.AES192, SymmetricKeyAlgorithm.AES128],
    compression=[CompressionAlgorithm.ZLIB, CompressionAlgorithm.BZ2, CompressionAlgorithm.ZIP, CompressionAlgorithm.Uncompressed])
    if pwd:
        key.protect(pwd, SymmetricKeyAlgorithm.AES256, HashAlgorithm.SHA256)
    return key

def write_pgp_enc_to_file(path, pgp_enc):
    f = open(path + "/.pk.txt", "w")
    f.write(str(pgp_enc))
    f.close()

def write_api_key_enc_to_file(path, data):
    f = open(path + "/.api_enc.txt", "w")
    f.write(str(data))
    f.close()

def write_ts_state_enc_to_file(path, data):
    f = open(path, "w")
    f.write(str(data))
    f.close()

def get_key_from_current_file(path):
    key, _ = pgpy.PGPKey.from_file(path + "/.pk.txt")
    return key

def get_encrypted_meta_data(path):
    f = open(path+ "/.api_enc.txt", "r")
    encrypted_data = f.read()
    f.close()
    return encrypted_data

def get_encrypted_ts_state(path):
    f = open(path, "r")
    encrypted_data = f.read()
    f.close()
    return encrypted_data

def get_encrypted_gsheet_meta_data(path):
    f = open(path+ "/.gsheet.txt", "r")
    encrypted_data = f.read()
    f.close()
    return encrypted_data

def get_decrypted_ts_state(path: str, pwd: str, key) -> dict:
    current_encrypted_data = get_encrypted_ts_state(path)
    with key.unlock(pwd):
        decrypted = pgpy_decrypt(key, current_encrypted_data).replace('\'', '\"')
        decrypted = decrypted.replace("None", "null")
        decrypted = decrypted.replace("True", "true")
        decrypted = decrypted.replace("False", "false")
        data = json.loads(decrypted)
        return data

def encrypt_and_write_ts_to_file(path: str, data: dict, key) -> None:
    encrypted = pgpy_encrypt(key, str(data))
    write_ts_state_enc_to_file(path, encrypted)

def add_keys_to_encrypted_file(path: str, exchange: str, pwd: str, key_information: dict, key):
    current_encrypted_data = get_encrypted_meta_data(path)
    with key.unlock(pwd):
        decrypted = pgpy_decrypt(key, current_encrypted_data).replace('\'', '\"')
        data = json.loads(decrypted)
        data[exchange] = key_information
        encrypted = pgpy_encrypt(key, str(data))
    write_api_key_enc_to_file(path, encrypted)

def modify_existing_key_in_encrypted_file(path: str, exchange: str, pwd: str, key_information: dict, key):
    current_encrypted_data = get_encrypted_meta_data(path)
    with key.unlock(pwd):
        decrypted = pgpy_decrypt(key, current_encrypted_data).replace('\'', '\"')
        data = json.loads(decrypted)
        if exchange not in data:
            raise KeyError
        data[exchange] = key_information
        encrypted = pgpy_encrypt(key, str(data))
    write_api_key_enc_to_file(path, encrypted)

def modify_key_name_in_encrypted_file(path: str, old_exchange_name: str, new_exchange_name: str, pwd: str, key):
    current_encrypted_data = get_encrypted_meta_data(path)
    with key.unlock(pwd):
        decrypted = pgpy_decrypt(key, current_encrypted_data).replace('\'', '\"')
        data = json.loads(decrypted)
        data[new_exchange_name] = data.pop(old_exchange_name)
        encrypted = pgpy_encrypt(key, str(data))
    write_api_key_enc_to_file(path, encrypted)

if __name__ == '__main__':
    pwd = getpass("provide password for pk:")
    current_path = os.getcwd()

    try:
        print("fetching pgp key")
        key, _ = pgpy.PGPKey.from_file(current_path + "/.pk.txt")
    
    except OSError as e:
        print("Creating pgpkey")
        key = pgpy_create_key(pwd)
        write_pgp_enc_to_file(current_path, key)

    key_information = {
        "Key":"",
        "Secret":"",
        'Other_fields': {}
    }

    # add_keys_to_encrypted_file(current_path, "Etherscan", pwd, key_information, key)

    # encrypted = pgpy_encrypt(key, str(key_information))

    # path = os.path.expanduser("~") + "/Documents/dev/enigma_capital/account_data_fetcher/.gsheet.txt"

    # write_ts_state_enc_to_file(path, encrypted)

    # with key.unlock(pwd):
    #     decrypted = pgpy_decrypt(key, encrypted)
    #     print(decrypted)
