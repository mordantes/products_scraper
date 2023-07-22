
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
from config import * 

class Manager:
    _encoding = 'cp1251'
    _ext = '.json'   
    
    @staticmethod
    def get_page(url:str,encoding:Optional[str] = None):
        
        if len(url) == 0:
            raise ValueError('error')
        data = requests.get(url)
        data.encoding =  encoding if encoding is not None else Manager._encoding
        return BeautifulSoup(data.text,features='html.parser')

    @staticmethod
    def extract_price(val:str):
        """Method to extract price from string with regex

        Args:
            val (str): string with numbers

        Returns:
            int type value
        """  
        normalize = val.split('.')
        get_numbers = re.findall('[0-9]+', normalize[0])
        return int(get_numbers[0])

    @staticmethod
    def save_to_file(data:list[dict], shop_name : str  ):
        os.path.exists(os.path.join(BASE_DIR)) or os.mkdir(BASE_DIR)
    
        save_name = '_'.join([shop_name,datetime.now().strftime("%Y-%m-%d")])


        with open(os.path.join(BASE_DIR, save_name) + Manager._ext, 'w' , encoding= 'utf-8') as f:
            try:
                dump(data, fp=f, ensure_ascii=False, indent=4)
            except IOError as ioe :
                print('Cant write data', ioe)
            except BaseException as e :
                print(e)
                
    @staticmethod
    def reverse_date(df:pd.DataFrame):
        def reverse(x:any):
            pd = x['parse_date']
            newd = pd.split('-')
            x['parse_date'] = '-'.join([newd[2], newd[1], newd[0]])
            return x 
    
        df = df.apply(reverse)
        return df 
    
    @staticmethod
    def to_csv(df:pd.DataFrame):

        all_data = pd.read_csv('res.csv')
        res = pd.concat([all_data, df])
        res.to_csv('res.csv')


    @staticmethod
    def to_click(df:pd.DataFrame):
        from clickhouse_driver import Client
        cli = Client(user=USER, password=PASSWORD, host=HOST, port=PORT, database=DB_NAME)
        df['offer'] = df['offer'].fillna(0).astype(int)
        df['parse_date'] = pd.to_datetime(df['parse_date'], format='%Y-%m-%d')

        cli.insert_dataframe(
            "insert into mybase.prods (name, price, offer, shopName, category, sub_category, parse_date ) VALUES",
            df,
            settings=dict(use_numpy=True)
        )
        print(f'Affected {len(df)} rows into clickhouse')

async def afactory(items: list, callback:callable, *args):
    """Create a async factory using each items to callback fn"""
    tasks = [] 
    for i in items :
        tasks.append(asyncio.create_task(callback(i, *args)))

    res = await asyncio.gather(*tasks)  

    return res 


async def fetch(url, session):
    async with session.get(url.get('href'), ssl=False) as r:
        if r.status != 200:
            print(f'Request failed, retrying...{url.get("href")}')
            return await fetch(url, session)
        text =  await r.text()
        page = BeautifulSoup(text,features='html.parser')
        url.update(content=page)
        return url
    
async def fetch_all(s, items: list[dict]):
    tasks = []
    for url in items:
        task = asyncio.create_task(fetch(s, url))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res



def arra(l:list) -> list:
    result = []
    for t in l :
        result = list(chain(result, t))
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
