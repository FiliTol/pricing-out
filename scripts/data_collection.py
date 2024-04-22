import os
import sqlite3
from sqlite3 import Cursor
import pandas as pd
import requests
import gzip
import shutil
import subprocess
import multiprocessing as mp
import time

import tqdm
from p_tqdm import p_map


os.chdir(os.path.dirname(os.path.abspath(__file__)))


def check_folder_exist():
    db_folder = r'../data/'
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
    tsv_original_folder = r'../data/timechain/original/'
    if not os.path.exists(tsv_original_folder):
        os.makedirs(tsv_original_folder)
    tsv_extracted_folder = r'../data/timechain/extracted/'
    if not os.path.exists(tsv_extracted_folder):
        os.makedirs(tsv_extracted_folder)


def create_table() -> None:
    conn: sqlite3.Connection = sqlite3.connect("../data/timechain.sqlite")
    cursor: Cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blocks(
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
    data_original_folder: str = "../data/timechain/original"
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv.gz"
    page: str = "https://gz.blockchair.com/bitcoin/blocks/"

    try:
        data = requests.get(f"{page}{template}{day}{extension}")
        with open(f"{data_original_folder}/{template}{day}{extension}", "wb") as f:
            f.write(data.content)
    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


def extract_gz(day: str) -> None:
    data_original_folder: str = "../data/timechain/original"
    data_extracted_folder: str = "../data/timechain/extracted"
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv.gz"

    try:
        with gzip.open(f"{data_original_folder}/{template}{day}{extension}", "rb") as f_in:
            with open(f"{data_extracted_folder}/{template}{day}.tsv", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    except Exception as err:
        print(f"An error occurred ({err}) for {day}")

    os.remove(f"{data_original_folder}/{template}{day}{extension}")


def insert_tsv(day: str) -> None:
    data_folder: str = "../data/timechain/extracted"
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv"
    mode: str = """.mode tabs"""
    insert: str = f""".import {data_folder}/{template}{day}{extension} blocks --skip 1"""

    try:
        subprocess.run(["sqlite3",
                        "../data/timechain.sqlite",
                        mode, insert])
        os.remove(f"{data_folder}/{template}{day}{extension}")

    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


if __name__ == "__main__":
    start_time = time.time()
    check_folder_exist()
    create_table()

    days = pd.date_range(start="2009-01-03", end="2009-04-21", freq="D").strftime("%Y%m%d").tolist()

    # with mp.Pool(10) as p:
    p_map(retrieve_day, days)
    # with mp.Pool(10) as p:
    p_map(extract_gz, days)
    for i in tqdm.tqdm(days):
        insert_tsv(i)

    print("--- %s seconds ---" % (time.time() - start_time))
