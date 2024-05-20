import gzip
import os
import shutil
import sqlite3
import subprocess
import time
from sqlite3 import Cursor

import re
from typing import List, Any

import pandas as pd
import requests
import tqdm
from p_tqdm import p_map
from pandas import DataFrame

from sys import setrecursionlimit
setrecursionlimit(100)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

db_table: str = "blocks"
db_folder: str = r"../data/"
db_file: str = f"{db_folder}timechain.sqlite"

tsv_original_folder: str = f"{db_folder}{db_table}/original/"
tsv_extracted_folder: str = f"{db_folder}{db_table}/extracted/"
original_extension: str = ".tsv.gz"
extracted_extension: str = ".tsv"

page: str = f"https://gz.blockchair.com/bitcoin/{db_table}/"
template: str = f"blockchair_bitcoin_{db_table}_"
start: str = "2009-01-03"
end: str = "2024-04-22"


def check_folder_exist():
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
    if not os.path.exists(tsv_original_folder):
        os.makedirs(tsv_original_folder)
    if not os.path.exists(tsv_extracted_folder):
        os.makedirs(tsv_extracted_folder)


def create_table() -> None:
    conn1: sqlite3.Connection = sqlite3.connect(db_file)
    cursor1: Cursor = conn1.cursor()
    cursor1.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_table} (
            id INTEGER PRIMARY KEY,
            hash TEXT,
            time TEXT,
            median_time TEXT,
            size INTEGER,
            stripped_size INTEGER,
            weight INTEGER,
            version INTEGER,
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
            fee_per_kb FLOAT,
            fee_per_kb_usd FLOAT,
            fee_per_kwu FLOAT,
            fee_per_kwu_usd FLOAT,
            cdd_total FLOAT,
            generation INTEGER,
            generation_usd FLOAT,
            reward INTEGER,
            reward_usd FLOAT,
            guessed_miner TEXT
        )
    """
    )
    conn1.commit()
    cursor1.close()
    conn1.close()


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
        os.remove(f"{tsv_original_folder}{template}{day}{original_extension}")

    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


def insert_tsv(day: str) -> None:
    # mode: str = """.mode tabs"""
    mode: str = """.separator \t"""
    insert: str = (
        f""".import {tsv_extracted_folder}{template}{day}{extracted_extension} blocks --skip 1"""
    )
    try:
        subprocess.run(["sqlite3", db_file, mode, insert])
    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


def insert_tsv_test(day: str) -> None:
    query = f"""INSERT INTO {db_table} (
    id, hash, time, median_time, size, stripped_size, weight, version, version_hex, version_bits,
    merkle_root, nonce,
    bits, difficulty, chainwork, coinbase_data_hex, transaction_count, witness_count, input_count, output_count,
    input_total, input_total_usd,
    output_total, output_total_usd, fee_total, fee_total_usd, fee_per_kb, fee_per_kb_usd, fee_per_kwu, fee_per_kwu_usd,
    cdd_total, generation,
    generation_usd, reward, reward_usd, guessed_miner
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?)
    """
    try:
        data: DataFrame = pd.read_table(f'{tsv_extracted_folder}{template}{day}{extracted_extension}',
                                        sep='\t',
                                        header=0,
                                        dtype=dict(id=int,
                                                   hash=str,
                                                   time=str,
                                                   median_time=str,
                                                   size=int,
                                                   stripped_size=int,
                                                   weight=int,
                                                   version=int,
                                                   version_hex=str,
                                                   version_bits=str,
                                                   merkle_root=str,
                                                   nonce=str,
                                                   bits=str,
                                                   difficulty=str,
                                                   chainwork=str,
                                                   coinbase_data_hex=str,
                                                   transaction_count=int,
                                                   witness_count=int,
                                                   input_count=int,
                                                   output_count=int,
                                                   input_total=int,
                                                   input_total_usd=float,
                                                   output_total=int,
                                                   output_total_usd=float,
                                                   fee_total=int,
                                                   fee_total_usd=float,
                                                   fee_per_kb=float,
                                                   fee_per_kb_usd=float,
                                                   fee_per_kwu=float,
                                                   fee_per_kwu_usd=float,
                                                   cdd_total=float,
                                                   generation=int,
                                                   generation_usd=float,
                                                   reward=int,
                                                   reward_usd=float,
                                                   guessed_miner=str
                                                   ))

        data.to_sql('blocks', conn, if_exists='append', index=False)

    except Exception as err:
        print(f"An error occurred ({err}) for {day}")


# The following two functions are used to accurately retrieve data from the source page.
# The reason for this is that the source page somehow returns empty files for certain days.


def check_empty() -> list:
    """
    Check if some files in the extracted folder are empty.
    If empty, removes them and adds their day a list.
    :return: list of empty days
    """
    empty_days: list = []

    for filename in os.listdir(tsv_extracted_folder):
        if os.path.getsize(f"{tsv_extracted_folder}{filename}") == 0:
            # extract the day from the filename
            pattern = r"\d{8}"
            day = re.findall(pattern, filename)[0]
            empty_days.append(day)
    print(len(empty_days))
    return empty_days


def remove_empty_files(empty_days: list) -> None:
    for day in empty_days:
        try:
            os.remove(f"{tsv_extracted_folder}{template}{day}{extracted_extension}")
        except Exception as err:
            print(f"An error occurred ({err}) for {day}")


def process() -> None:
    """
    Contains all the functions to be executed in order to retrieve data
    and adjust possible empty files.
    """
    # First data crawling
    #days = pd.date_range(start=start, end=end, freq="D").strftime("%Y%m%d").tolist()
    #for day in ['20090104', '20090105', '20090106', '20090107', '20090108']:
    #    days.remove(day)
    #p_map(retrieve_day, days)
    #p_map(extract_gz, days)
    empty_days = check_empty()

    # Recursion to correct empty files
    #while len(empty_days) > 0:
    #    remove_empty_files(empty_days)
    #    for day in empty_days:
    #        p_map(retrieve_day, day)
    #        p_map(extract_gz, day)
    #    empty_days = check_empty()

    #for i in tqdm.tqdm(days):
    #    insert_tsv_test(i)
    #    if days.index(i) % 100 == 0:
    #        conn.commit()
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    start_time = time.time()
    check_folder_exist()
    create_table()
    conn: sqlite3.Connection = sqlite3.connect(db_file)
    cursor: Cursor = conn.cursor()
    
    check_empty()
    process()
    
    cursor.close()
    conn.close()
