import asyncio
from datetime import datetime
from itertools import chain
from json import dump
import os
from queue import Empty
from typing import Optional
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import httpx

from clickhouse_driver import Client
from config import BASE_DIR, C_DB, C_HOST, C_TABLE, C_USER, C_PORT, C_PWD


class Manager:
    _encoding = "cp1251"
    _ext = ".json"

    @staticmethod
    def get_page(url: str, encoding: Optional[str] = None):
        if len(url) == 0:
            raise ValueError("error")
        data = requests.get(url)
        data.encoding = encoding if encoding is not None else Manager._encoding
        return BeautifulSoup(data.text, features="html.parser")

    @staticmethod
    def extract_price(val: str):
        """Method to extract price from string with regex

        Args:
            val (str): string with numbers

        Returns:
            int type value
        """
        normalize = val.split(".")
        get_numbers = re.findall("[0-9]+", normalize[0])
        return int(get_numbers[0])

    @staticmethod
    def save_to_file(data: list[dict], shop_name: str):
        os.path.exists(os.path.join(BASE_DIR)) or os.mkdir(BASE_DIR)

        save_name = "_".join([shop_name, datetime.now().strftime("%Y-%m-%d")])

        with open(
            os.path.join(BASE_DIR, save_name) + Manager._ext, "w", encoding="utf-8"
        ) as f:
            try:
                dump(data, fp=f, ensure_ascii=False, indent=4)
            except IOError as ioe:
                print("Cant write data", ioe)
            except BaseException as e:
                print(e)

    @staticmethod
    def reverse_date(df: pd.DataFrame):
        def reverse(x: any):
            pd = x["parse_date"]
            newd = pd.split("-")
            x["parse_date"] = "-".join([newd[2], newd[1], newd[0]])
            return x

        df = df.apply(reverse)
        return df

    @staticmethod
    def to_csv(df: pd.DataFrame):
        all_data = pd.read_csv("res.csv")
        res = pd.concat([all_data, df])
        res.to_csv("res.csv")

    @staticmethod
    def to_click(df: pd.DataFrame):
        try:
            cli = Client(
                user=C_USER, password=C_PWD, host=C_HOST, port=C_PORT, database=C_DB
            )
            df["offer"] = df["offer"].fillna(0).astype(int)
            df["parse_date"] = pd.to_datetime(df["parse_date"], format="%Y-%m-%d")

            cli.insert_dataframe(
                f"insert into {C_DB}.{C_TABLE} (name, price, offer, shop_name, category, sub_category, parse_date ) VALUES",
                df,
                settings=dict(use_numpy=True),
            )
            print(f"Affected {len(df)} rows into clickhouse")
        except BaseException as e:
            print(str(e))


async def asyncio_task_factory(items: list, callback: callable, *args):
    """Create a async factory using each items to callback fn"""
    tasks = []
    done = []

    for i in items:
        tasks.append(asyncio.create_task(callback(i, *args)))

        if len(tasks) == 20:
            res = await asyncio.gather(*tasks)
            done = [*done, *res]
            tasks = list()

    return res


async def get_data(
    client:httpx.AsyncClient,
    url: dict,
):
    resp = await client.get(url.get('href'))

    # print(resp.status_code, url.get('href'))
    if resp.status_code == 200:
        url.update(content=resp.text)
        return url
    elif resp.status_code == 404 :
        return None
    else :
        return await get_data(client, url)


async def fetch_gather(url_list: list[dict], chunksize: Optional[int] = 5):
    tasks = []
    done = []
    timeout = httpx.Timeout(None, connect=5)
    async with httpx.AsyncClient(verify=False,timeout=timeout ) as client:
        for idx, i in enumerate(url_list, start=1):

            tasks.append(asyncio.create_task(get_data(client, i)))

            if len(tasks) == chunksize or idx == len(url_list):
                temp_result = await asyncio.gather(*tasks)
                print(f"Progress tasks/total - {idx}/{len(url_list)}, shopname -> {i.get('shop_name')}")
                temp_result = [item for item in temp_result if item is not None]
                done = [*done, *temp_result]
                tasks = list()
    
    for item in done:
        page = BeautifulSoup(item.get('content'), features="lxml")
        item.update(content=page)

    return done


async def fetch_all(s, items: list[dict]):
    tasks = []
    for url in items:
        task = asyncio.create_task(fetch_gather(s, url))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res


def array_spread(l: list) -> list:
    result = []
    for t in l:
        if isinstance(t, list):
            result = [*result, *t]
        else :
            result.append(t)
    return result


def run_async_function(async_func, result_queue):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_func(result_queue))


def get_results(result_queue):
    results = []
    while True:
        try:
            result = result_queue.get()
            results.append(result)
        except Empty:
            break
    return results
