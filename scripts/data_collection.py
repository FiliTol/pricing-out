import os
import subprocess
import sqlite3
import csv
from sqlite3 import Cursor


def check_folder_exist():
    # Check data folder
    db_folder = r'../data/'
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
    # Check timechain folder
    tsv_folder = r'../data/timechain/'
    if not os.path.exists(tsv_folder):
        os.makedirs(tsv_folder)


def drop_blocs_table() -> None:
    conn: sqlite3.Connection = sqlite3.connect("../data/timechain.sqlite")
    cursor: Cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS blocks")
    conn.commit()
    cursor.close()
    conn.close()


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
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv.gz"
    page: str = "https://gz.blockchair.com/bitcoin/blocks/"

    try:
        day_data: subprocess.CompletedProcess = subprocess.run(
            f"wget -P ../data/timechain {page}{template}{day}{extension} && gzip -d ../data/timechain/{template}{day}{extension}",
            shell=True,
            executable="/bin/bash",
        )
    except:
        BaseException


def insert_tsv():
    data_folder: str = os.path.relpath("../data/timechain")
    ls: list = os.listdir(data_folder)

    conn: sqlite3.Connection = sqlite3.connect("../data/timechain.sqlite")
    cursor: Cursor = conn.cursor()

    for el in ls:
        mode = """.mode tabs"""
        insert = f""".import ../data/timechain/{el} blocks --skip 1"""
        #cursor.execute(mode)
        #cursor.execute(insert)
        subprocess.run(["sqlite3", "../data/timechain.sqlite", mode, insert])

    conn.commit()
    cursor.close()
    conn.close()


check_folder_exist()
create_table()
#retrieve_day("20240415")
insert_tsv()
