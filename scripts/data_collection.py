import os
import subprocess
import sqlite3
from sqlite3 import Cursor
import pandas as pd
from tqdm.auto import tqdm


def check_folder_exist():
    # Check data folder
    db_folder = r'../data/'
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
    # Check timechain folder
    tsv_folder = r'../data/timechain/'
    if not os.path.exists(tsv_folder):
        os.makedirs(tsv_folder)


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
    data_folder: str = "../data/timechain"
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv.gz"
    page: str = "https://gz.blockchair.com/bitcoin/blocks/"

    try:
        day_data: subprocess.CompletedProcess = subprocess.run(
            f"wget -P {data_folder} {page}{template}{day}{extension} && gzip -d {data_folder}/{template}{day}{extension}",
            shell=True,
            executable="/bin/bash",
        )
    except:
        print(f"No blocks on day {day}")


def insert_tsv(day: str) -> None:
    data_folder: str = "../data/timechain"
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv"
    mode: str = """.mode tabs"""
    insert: str = f""".import {data_folder}/{template}{day}{extension} blocks --skip 1"""
    try:
        subprocess.run(["sqlite3", "../data/timechain.sqlite", mode, insert])
    except:
        print(f"Error in inserting tsv for {day}")


def retrieve_all() -> None:
    days: list = pd.date_range(start="2009-01-03", end="2009-02-03", freq="D").strftime("%Y%m%d").tolist()
    for i in tqdm(days):
        retrieve_day(i)
        insert_tsv(i)

        try:
            os.remove(f"../data/timechain/blockchair_bitcoin_blocks_{i}.tsv")
        except:
            print(f"Error in removing tsv for {i}")


check_folder_exist()
create_table()
retrieve_all()
