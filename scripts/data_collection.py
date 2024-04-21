import os
import sqlite3
from sqlite3 import Cursor
import pandas as pd
import aiohttp
import asyncio
import gzip
import shutil
import subprocess

os.chdir(os.path.dirname(os.path.abspath(__file__)))


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


async def retrieve_day_async(day: str) -> None:
    data_folder: str = "../data/timechain"
    template: str = "blockchair_bitcoin_blocks_"
    extension: str = ".tsv.gz"
    page: str = "https://gz.blockchair.com/bitcoin/blocks/"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{page}{template}{day}{extension}") as response:
                day_data = await response.read()
                with open(f"{data_folder}/{template}{day}{extension}", "wb") as f:
                    f.write(day_data)
                with gzip.open(f"{data_folder}/{template}{day}{extension}", "rb") as f_in:
                    with open(f"{data_folder}/{template}{day}.tsv", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
    except aiohttp.ClientError as client_err:
        print(f"HTTP error occurred ({client_err}) for {day}")
    except Exception as err:
        print(f"An error occurred ({err}) for {day}")

    os.remove(f"{data_folder}/{template}{day}{extension}")


def insert_tsv(day: str) -> None:
    data_folder: str = "../data/timechain"
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


async def retrieve_all_async() -> None:
    days: list = pd.date_range(start="2009-01-03", end="2009-02-03", freq="D").strftime("%Y%m%d").tolist()
    tasks = []
    for i in days:
        try:
            tasks.append(asyncio.create_task(retrieve_day_async(i)))
            insert_tsv(i)

        except Exception as e:
            print(f"Error in removing tsv for {i}")

        if i == "20240420":
            break

    await asyncio.gather(*tasks)


async def main() -> None:
    await retrieve_all_async()


check_folder_exist()
create_table()
asyncio.run(main())
