import asyncio
from datetime import datetime
from itertools import chain
import time

from src.helper import (
    Manager,
    array_spread,
    fetch_concurrent_thread,
)

URL = "https://gastore.ru"


def get_menu():
    page = Manager.get_page(URL, "utf-8")
    menu_items = [
        (i.attrs["href"], i.select("span.name")[0].get_text())
        for i in page.select("div.catalog_block>ul>li>a")
    ]
    menu_links = list()
    for sub_category in menu_items:
        sub_menu_data = Manager.get_page(URL + sub_category[0], encoding="utf-8")
        sub_menu_links = sub_menu_data.select("div.item>div.name>a")
        for item in sub_menu_links:
            menu_links.append(
                dict(
                    href=URL + item.attrs["href"] + "?SHOWALL_2=1",
                    category=sub_category[1],
                    sub_category=str(item.get_text()).strip().lower(),
                )
            )
    for i in menu_links:
        i.update(shop_name="gastore")
    return menu_links


def parse_from_link(page, item):
    result: list = list()
    page_items = page.select("div.item_info")
    products = list()
    products_dto = list()
    for card in page_items:
        try:
            products.append(
                (
                    card.select("div.item-title")[0].get_text().replace("\n", ""),
                    Manager.extract_price(
                        card.select("div.price")[0]
                        .get_text()
                        .replace("\n", "")
                        .replace(" ", "")
                    ),
                )
            )
        except:
            print(card.select("div.price"))
            raise ValueError(f"Not valid card {page_items}")

    for prod in products:
        products_dto.append(
            dict(
                name=prod[0],
                price=prod[1],
                category=item["category"].replace("\n", ""),
                sub_category=item["sub_category"].replace("\n", "").lower(),
                offer=None,
                shop_name=item["shop_name"],
                parse_date=datetime.now().strftime("%Y-%m-%d"),
            )
        )
    result = list(chain(result, products_dto))
    return result


def parse_gastore():
    try:
        menu = get_menu()
        result = []
        product_pages = fetch_concurrent_thread(menu)
        for item in product_pages:
            result.append(parse_from_link(item.get("content"), item))

        result = array_spread(result)

        return result
    except BaseException as e:
        print("Error", str(e))
        raise e


if __name__ == "__main__":
    start = time.time()
    asyncio.run(parse_gastore())
    print(f"Done for {time.time() - start } ")
