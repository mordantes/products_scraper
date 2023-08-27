from datetime import datetime
from json import dump
import os
import time
from typing import Callable, Optional
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests

from clickhouse_driver import Client
from config import BASE_DIR, C_DB, C_HOST, C_TABLE, C_USER, C_PORT, C_PWD


from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


class Manager:
    """Class with usefull common functions"""

    _ext = ".json"

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
        """Save list of parsed items into file

        Args:
            data (list[dict]): input list
            shop_name (str): _description_
        """
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
        """Dataframe apply function to reverse old format files date value
        Args:
            df (pd.DataFrame): input dataframe
        """

        def reverse(x: any):
            pd = x["parse_date"]
            newd = pd.split("-")
            x["parse_date"] = "-".join([newd[2], newd[1], newd[0]])
            return x

        df = df.apply(reverse)
        return df

    @staticmethod
    def to_csv(df: pd.DataFrame):
        """Save input dataframe into .csv file , by default ``res.csv`` name

        Args:
            df (pd.DataFrame): input dataframe
        """
        all_data = pd.read_csv("res.csv")
        res = pd.concat([all_data, df])
        res.to_csv("res.csv")

    #!TODO unwrap normalize method into separate function
    @staticmethod
    def to_click(df: pd.DataFrame, format: Optional[str] = None):
        """Load into Clickhouse DWH input dataframe

        Args:
            df (pd.DataFrame): input dataframe
        """
        try:
            cli = Client(
                user=C_USER, password=C_PWD, host=C_HOST, port=C_PORT, database=C_DB
            )
            df["offer"] = df["offer"].fillna(0).astype(int)
            df["parse_date"] = pd.to_datetime(df["parse_date"], format= format or "%Y-%m-%d")

            cli.insert_dataframe(
                f"insert into {C_DB}.{C_TABLE} (name, price, offer, shop_name, category, sub_category, parse_date ) VALUES",
                df,
                settings=dict(use_numpy=True),
            )
            print(f"Affected {len(df)} rows into clickhouse")
        except BaseException as e:
            print(str(e))


def get_data_sync(
    url: dict,
) -> Optional[dict]:
    """Get text data from given url page (!recursive if status responce <400)

    Args:
        url (dict): item object, property 'href' in neccessary

    Returns:
        dict: append property 'content' with html text data from page
    """
    resp = requests.get(url.get("href"))

    if resp.status_code == 200:
        text = resp.text
        # print(resp.encoding)
        text = text.encode(resp.encoding)
        # if resp.encoding == 'cp1251' :
        #     text = text.encode('ISO-8859-1')
        # else :
        #     text = text.encode('utf-8')
        url.update(content=text)
        print(f'Done result {url.get("href")}')
        return url
    elif resp.status_code == 404:
        return None
    else:
        print(f"Error when try to get data, retry...")
        time.sleep(0.1)
        return get_data_sync(url)


def fetch_concurrent_process(url_list: list[dict]) -> list[dict]:
    """Run scraper func by every item in list in process pool

    Args:
        url_list (list[dict]): neccessary dict keys - 'href'

    Returns:
        list[dict]: append property 'content' with html data from page and return updated list of dicts
    """
    with ProcessPoolExecutor(os.cpu_count() - 1) as worker:
        done = worker.map(get_data_sync, url_list)

    done = [item for item in done if item is not None]

    for item in done:
        page = BeautifulSoup(item.get("content"), features="lxml")
        item.update(content=page)

    return done


def fetch_concurrent_thread(url_list: list[dict]) -> list[dict]:
    """Run scraper func by every item in list in thread pool

    Args:
        url_list (list[dict]): neccessary dict keys - 'href'

    Returns:
        list[dict]: append property 'content' with html data from page and return updated list of dicts
    """
    with ThreadPoolExecutor(os.cpu_count() - 1) as worker:
        done = worker.map(get_data_sync, url_list)

    done = [item for item in done if item is not None]

    for item in done:
        page = BeautifulSoup(item.get("content"), features="lxml")
        item.update(content=page)

    return done


def array_spread(l: list[list]) -> list:
    """Custom spread list of lists into single one

    Args:
        l (list): input list[list]

    Returns:
        list: result list
    """
    result = []
    for t in l:
        if isinstance(t, list):
            result = [*result, *t]
        else:
            result.append(t)
    return result


def make_executable(func: Callable):
    """Function-hack to pass into PoolExecutor list of functions insted of list of items to execute by each of them

    Args:
        func (Callable): function to execute

    Returns:
        <T>: return result from executed function
    """
    return func()
