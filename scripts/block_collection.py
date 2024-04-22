import gzip
import os
import shutil
import sqlite3
import subprocess
import time
from sqlite3 import Cursor

import pandas as pd
import requests
import tqdm
from p_tqdm import p_map

os.chdir(os.path.dirname(os.path.abspath(__file__)))

db_table: str = "blocks"
db_folder: str = r"../data/"
db_file: str = f"{db_folder}timechain.sqlite"
tsv_original_folder: str = f"{db_folder}{db_table}/timechain/original/"
tsv_extracted_folder: str = f"{db_folder}{db_table}/timechain/extracted/"
original_extension: str = ".tsv.gz"
extracted_extension: str = ".tsv"

page: str = f"https://gz.blockchair.com/bitcoin/{db_table}/"
template: str = f"blockchair_bitcoin_{db_table}_"
start: str = "2009-01-03"
end: str = "2024-04-21"


def check_folder_exist():
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
    if not os.path.exists(tsv_original_folder):
        os.makedirs(tsv_original_folder)
    if not os.path.exists(tsv_extracted_folder):
        os.makedirs(tsv_extracted_folder)


def create_table() -> None:
    conn: sqlite3.Connection = sqlite3.connect(db_file)
    cursor: Cursor = conn.cursor()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_table} (
            id INTEGER PRIMARY KEY,
            hash TEXT,
            time DATETIME,
            median_time DATETIME,
            size TEXT,
            stripped_size TEXT,
            weight TEXT,
            version TEXT,
            version_hex TEXT,
            version_bits TEXT,
            merkle_root TEXT,
            nonce TEXT,
            bits TEXT,
            difficulty TEXT,
            chainwork TEXT,
            coinbase_data_hex TEXT,
            transaction_count INTEGER,
            witness_count INTEGER,
            input_count INTEGER,
            output_count INTEGER,
            input_total INTEGER,
            input_total_usd FLOAT,
            output_total INTEGER,
            output_total_usd FLOAT,
            fee_total INTEGER,
            fee_total_usd FLOAT,
            fee_per_kb INTEGER,
            fee_per_kb_usd FLOAT,
            fee_per_kwu INTEGER,
            fee_per_kwu_usd FLOAT,
            cdd_total INTEGER,
            generation INTEGER,
            generation_usd FLOAT,
            reward INTEGER,
            reward_usd FLOAT,
            guessed_miner TEXT
        )
    """
    )
    conn.commit()
    cursor.close()
    conn.close()


def retrieve_day(day: str) -> None:

    try:
        data = requests.get(f"{page}{template}{day}{original_extension}")
        with open(
            f"{tsv_original_folder}{template}{day}{original_extension}", "wb"
        ) as f:
            f.write(data.content)
    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


def extract_gz(day: str) -> None:

    try:
        with gzip.open(
            f"{tsv_original_folder}{template}{day}{original_extension}", "rb"
        ) as f_in:
            with open(
                f"{tsv_extracted_folder}{template}{day}{extracted_extension}", "wb"
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)

    except Exception as err:
        print(f"An error occurred ({err}) for {day}")

    os.remove(f"{tsv_original_folder}{template}{day}{original_extension}")


def insert_tsv(day: str) -> None:
    mode: str = """.mode tabs"""
    insert: str = (
        f""".import {tsv_extracted_folder}{template}{day}{extracted_extension} blocks --skip 1"""
    )
    try:
        subprocess.run(["sqlite3", db_file, mode, insert])
        os.remove(f"{tsv_extracted_folder}{template}{day}{extracted_extension}")
    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


if __name__ == "__main__":
    start_time = time.time()
    check_folder_exist()
    create_table()

    days = pd.date_range(start=start, end=end, freq="D").strftime("%Y%m%d").tolist()
    p_map(retrieve_day, days)
    p_map(extract_gz, days)
    for i in tqdm.tqdm(days):
        insert_tsv(i)

    print("--- %s seconds ---" % (time.time() - start_time))
