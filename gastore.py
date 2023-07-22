


import asyncio
from datetime import datetime
from itertools import chain
import json
import time
from typing import Optional

import aiohttp

from helper import Manager, afactory, arra, fetch

URL ='http://gastore.ru'


def get_menu():
    page = Manager.get_page(URL,'utf-8')
    menu_items = [(i.attrs['href'], i.select('span.name')[0].get_text()) for i in page.select('div.catalog_block>ul>li>a')] 
    menu_links = list()
    for sub_category in menu_items:
        sub_menu_data = Manager.get_page(URL + sub_category[0], encoding='utf-8')
        sub_menu_links = sub_menu_data.select('div.item>div.name>a')
        for item in sub_menu_links:
            menu_links.append(
                dict(
                    href = URL + item.attrs['href'] + '?SHOWALL_2=1',
                    category= sub_category[1],
                    sub_category=str(item.get_text()).strip().lower()
                )
            )
    return menu_links


def parse_from_link(page, item):
    result:list = list()
    page_data = Manager.get_page(item['href'], encoding='utf-8')
    page_items = page_data.select('div.item_info')
    products = list()
    products_dto = list()
    for card in page_items:
        products.append((
            card.select('div.item-title')[0].get_text().replace('\n', ''),
            Manager.extract_price(card.select('div.price')[0].get_text().replace('\n', '').replace(' ', ''))
        ))
    for prod in products:
        products_dto.append(dict(
            name=prod[0],
            price=prod[1],
            category= item['category'].replace('\n', ''),
            sub_category= item['sub_category'].replace('\n', '').lower(),
            offer= None,
            shopName='gastore',
            parse_date= datetime.now().strftime("%Y-%m-%d")
        )
        )
    result = list(chain(result, products_dto))
    return result



async def parse_gastore(result_queue : Optional[any] = None):
    try :
        menu = get_menu()
        result = []
        async with aiohttp.ClientSession() as session:
            htmls = await afactory(menu, fetch, session)
        for item in htmls :
            result.append(parse_from_link(item.get('content'), item))

        result = arra(result)    

        if result_queue :
            await result_queue.put(result)
        else :
            return result
    except :
        print('Error')
    

if __name__ == '__main__':
    start = time.time()
    asyncio.run(parse_gastore())
    print(f'Done for {time.time() - start } ')