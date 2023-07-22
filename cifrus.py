import asyncio
from datetime import datetime
import json
import time
from typing import Optional
import aiohttp
from helper import Manager, afactory, arra, fetch


URL = 'https://www.cifrus.ru'
_deprecated = ['/catalog/1', '/catalog/3', '/catalog/26', '/catalog/19', '/remont',  '/catalog/64']


def get_menu(url) -> list[dict]:
    page = Manager.get_page(url)
    menu_elements = page.find_all(class_='dropdown-submenu')
    menu_items = list()
    for item in menu_elements:
        main_menu_item = item.find(class_='dropdown-toggle')
        item_props = dict(
            href= URL + main_menu_item.attrs['href'],
            category=main_menu_item.get_text(),
            sub_category= 'cifrus_sub_cat'
        )
        sub_menu_items = item.select(
            'div.dropdown-menu>div.dropdown-inner>ul>li>a')
        if not main_menu_item.attrs['href'] in _deprecated:
            if sub_menu_items is not None and len(sub_menu_items) > 0:
                for elem in sub_menu_items:
                    menu_items.append(
                        dict(
                            href= URL + elem.attrs['href'],
                            sub_category=elem.get_text(),
                            category=item_props['category']
                        )
                    )
            else:
                menu_items.append(item_props)
    return menu_items


async def get_items(page):
    pagination = list(
        category_item for category_item in page.get('content').find_all(class_='list-group-item')
    )
    if bool(pagination) and len(pagination) > 1:
        items = []
        for paginated in pagination:
            if str(paginated.attrs['href'])[-2:] != '//':
                current_href = URL + paginated.attrs['href']
                item = dict(
                    href = current_href,
                    category = page['category'],
                    sub_category = page['sub_category']
                )
                items.append(item)

        async with aiohttp.ClientSession() as session:
            phones = await afactory(items, fetch, session)
        spreaded = [parse_cards(t.get('content'),page) for t in phones]
        return arra(spreaded)
    else :
        return parse_cards(page.get('content'), page)

def parse_cards(page, args):
    result = list()
    parse_date = datetime.now().strftime("%Y-%m-%d")
    cards = page.find_all(class_='product-thumb')
    for products in cards:
        product_name = products.find(class_='name').find('a')
        product_price_old = products.find(class_='price-old')
        product_price_new = products.find(class_='price-new')
        if product_name and (product_price_new or product_price_old):
            product = {
                "name": product_name.get_text(),
                "price":  Manager.extract_price(product_price_new.get_text()) if product_price_new else Manager.extract_price(product_price_old.get_text()),
                "offer":  Manager.extract_price(product_price_new.get_text()) if not product_price_new else None,
                "parse_date": parse_date,
                'category': str(args.get('category')),
                'sub_category': str(args.get('sub_category')).lower(),
                'shopName': 'cifrus'
            }
            result.append(product)
    return result

async def parse_cifrus(result_queue : Optional[any] = None):
    # initally get left-side menu items with their sub_items values
    # into one huge list of urls like [{ href : '...', category : '[main_name]', sub_category : '[sub_item_name]''}]
    menu = get_menu(URL)
    # using async get content from each page
    async with aiohttp.ClientSession() as session:
        htmls = await afactory(menu, fetch, session)
        
    data = await afactory(htmls,get_items)

    data = arra(data)

    if result_queue :
        await result_queue.put(data)
    else :
        return data

if __name__ == '__main__':
    start = time.time()
    asyncio.run(parse_cifrus())
    print(f'Done for {time.time() - start } ')