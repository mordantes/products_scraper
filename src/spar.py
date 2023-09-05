from datetime import datetime
from itertools import chain
import time

from bs4 import BeautifulSoup


from src.helper import (
    Manager,
    array_spread,
    fetch_concurrent_thread,
    get_data_sync,
)

URL = "https://spar-online.ru"
ALL = "?SHOWALL_1=1"


def get_menu(page: dict):
    """Function to get list of category items (dropdown menu commonly) from a page and obtaint their URL adress
    that will be a path to product cards

    Args:
        page (dict): target object

    Returns:
        list[dict]: result dict with ``href`` , ``sub_category``, ``category``, ``shop_name`` keys
    """
    page = BeautifulSoup(page.get("content"), features="lxml")
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


def parse_cards(item):
    """Obtain product's categories from a given page

    Args:
        item (dict): input object

    Returns:
        list[dict]: all finded card-object item parameters
    """
    _result = []
    curr_page = item.get("content")
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
                shop_name=item["shop_name"],
                parse_date=datetime.now().strftime("%Y-%m-%d"),
            )
        )
    return _result


def parse_spar():
    """
    Main func to obtain all items from cifrus.ru web-site

    Returns:
        list[dict]: parsed product's cards of all items that exists in shop
    """
    # get main page html-data
    page = get_data_sync({"href": URL})
    # parse from html list of category-URL's
    menu = get_menu(page)
    # fetch html content from every URL's menu
    result = fetch_concurrent_thread(menu, parse_cards)
    # parse from list of html-content page's product's data
    return result


if __name__ == "__main__":
    t0 = time.perf_counter()
    parse_spar()
    print(f"Done for {time.perf_counter() - t0 } ")
