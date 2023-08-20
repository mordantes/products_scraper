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

# main ulr of a shop
URL = "https://gastore.ru"


def get_menu():
    """Function to get list of category items (dropdown menu commonly) from a page and obtaint their URL adress
    that will be a path to product cards

    Args:
        url (str): target URL

    Returns:
        list[dict]: result dict with ``href`` , ``sub_category``, ``category``, ``shop_name`` keys
    """
    page = get_data_sync({"href": URL})
    page = BeautifulSoup(page.get("content"), features="lxml")

    menu_items = [
        (i.attrs["href"], i.select("span.name")[0].get_text())
        for i in page.select("div.catalog_block>ul>li>a")
    ]
    menu_links = list()
    for sub_category in menu_items:
        sub_menu_data = get_data_sync({"href": URL + sub_category[0]})
        sub_menu_data = BeautifulSoup(sub_menu_data.get("content"), features="lxml")
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


def parse_card(item):
    """Obtain product's categories from a given page

    Args:
        page (dict): input object

    Returns:
        list[dict]: all finded card-object item parameters
    """
    result: list = list()
    page = item.get("content")
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
            print(str(ValueError(f"Not valid card {card}")))
            continue

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
    """
    Main func to obtain all items from gastore.ru web-site

    Returns:
        list[dict]: parsed product's cards of all items that exists in shop
    """
    result = []
    # let list of category-url pages to get product's datafrom them
    menu = get_menu()
    # fetch a html-content from them
    product_pages = fetch_concurrent_thread(menu)
    # parse product's data from html
    for item in product_pages:
        result.append(parse_card(item))
    # spread list of lists to single one
    result = array_spread(result)

    return result


if __name__ == "__main__":
    start = time.time()
    parse_gastore()
    print(f"Done for {time.time() - start } ")
