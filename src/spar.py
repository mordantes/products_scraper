import asyncio
from datetime import datetime
from itertools import chain
import time


from src.helper import (
    Manager,
    array_spread,
    fetch_concurrent_thread,
)

URL = "https://spar-online.ru"
ALL = "?SHOWALL_1=1"


def parse_links(page):
    menu_block = page.select("ul.menu>li")
    menu_list = list()

    for item in menu_block:
        if item.select_one("a>span.name") is not None:
            sub_menu_block = item.select_one("div.dropdown")
            if sub_menu_block is not None:
                sub_menu = item.select("ul.left-menu-wrapper>li>a")
                menu_list = list(
                    chain(
                        menu_list,
                        [
                            dict(
                                sub_category=_item.select_one("span").get_text()
                                if _item.select_one("span") is not None
                                else "spar_sub_cat",
                                href=URL + _item.attrs["href"] + ALL,
                                category=item.select_one("a>span.name").get_text(),
                            )
                            for _item in sub_menu
                            if _item is not None
                        ],
                    )
                )
            else:
                menu_list.append(
                    dict(
                        category=item.select_one("a>span.name").get_text(),
                        sub_category="spar_sub_cat",
                        href=URL + item.select_one("a").attrs["href"] + ALL,
                    )
                )

    for i in menu_list:
        i.update(shop_name="spar")
    return menu_list


def parse_cards(curr_page, item):
    _result = []
    product_cards = curr_page.select("div.item_info")
    for product in product_cards:
        name = product.select_one("div.item-title>a>span").get_text()
        prices = tuple(
            [
                price_element.get_text()
                for price_element in product.select(
                    "span.values_wrapper>span.price_value"
                )
            ]
        )
        _result.append(
            dict(
                name=name,
                category=str(item["category"]),
                sub_category=item["sub_category"].lower(),  # check if validated ^
                price=Manager.extract_price(prices[0]),
                offer=Manager.extract_price(prices[1]) if len(prices) == 2 else None,
                shopName=item["shop_name"],
                parse_date=datetime.now().strftime("%Y-%m-%d"),
            )
        )
    return _result


def parse_spar():
    page = Manager.get_page(URL, "utf-8")
    menu = parse_links(page)
    result = []
    product_pages = fetch_concurrent_thread(menu)
    for i in product_pages:
        result.append(parse_cards(i.get("content"), i))
    result = array_spread(result)

    return result


if __name__ == "__main__":
    t0 = time.perf_counter()
    asyncio.run(parse_spar())
    print(f"Done for {time.perf_counter() - t0 } ")
