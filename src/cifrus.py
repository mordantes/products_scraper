from datetime import datetime
import time

from bs4 import BeautifulSoup
from src.helper import Manager, array_spread, fetch_concurrent_thread, get_data_sync

# main url of shop
URL = "https://www.cifrus.ru"
# list of menu items that deprecated to parse content
_deprecated = [
    "/catalog/1",
    "/catalog/3",
    "/catalog/26",
    "/catalog/19",
    "/remont",
    "/catalog/64",
]


def get_menu(url: str) -> list[dict]:
    """Function to get list of category items (dropdown menu commonly) from a page and obtaint their URL adress
    that will be a path to product cards

    Args:
        url (str): target URL

    Returns:
        list[dict]: result dict with ``href`` , ``sub_category``, ``category``, ``shop_name`` keys
    """
    page = get_data_sync({"href": url})
    page = BeautifulSoup(page.get("content"), features="lxml")
    menu_elements = page.find_all(class_="dropdown-submenu")
    menu_items = list()
    for item in menu_elements:
        main_menu_item = item.find(class_="dropdown-toggle")
        item_props = dict(
            href=URL + main_menu_item.attrs["href"],
            category=main_menu_item.get_text(),
            sub_category="cifrus_sub_cat",
        )
        sub_menu_items = item.select("div.dropdown-menu>div.dropdown-inner>ul>li>a")
        if not main_menu_item.attrs["href"] in _deprecated:
            if sub_menu_items is not None and len(sub_menu_items) > 0:
                for elem in sub_menu_items:
                    menu_items.append(
                        dict(
                            href=URL + elem.attrs["href"],
                            sub_category=elem.get_text(),
                            category=item_props["category"],
                        )
                    )
            else:
                menu_items.append(item_props)

    for i in menu_items:
        i.update(shop_name="cifrus")
    return menu_items


def get_true_hrefs(page: dict) -> list[dict] | dict:
    """For this type of shop need to parse every menu items by a subcategory menu items
    to obtain true href that will display product's cards

    Args:
        page (dict): result dict from ``get_menu`` funct

    Returns:
        list[dict] | dict: if given url content have left-side menu, return type is list, otherwhere single dict item
    """
    pagination = list(
        category_item
        for category_item in page.get("content").find_all(class_="list-group-item")
    )
    if bool(pagination) and len(pagination) > 1:
        items = []
        for paginated in pagination:
            if str(paginated.attrs["href"])[-2:] != "//":
                current_href = URL + paginated.attrs["href"]
                item = dict(
                    shop_name=page["shop_name"],
                    content=None,
                    href=current_href,
                    category=page["category"],
                    sub_category=page["sub_category"],
                )
                items.append(item)
        return items
    else:
        return page


def parse_card(page) -> list[dict]:
    """Obtain product's categories from a given page

    Args:
        page (dict): input object

    Returns:
        list[dict]: all finded card-object item parameters
    """
    result = list()
    parse_date = datetime.now().strftime("%Y-%m-%d")
    cards = page.get("content").find_all(class_="product-thumb")
    for products in cards:
        product_name = products.find(class_="name").find("a")
        product_price_old = products.find(class_="price-old")
        product_price_new = products.find(class_="price-new")
        if product_name and (product_price_new or product_price_old):
            product = {
                "name": product_name.get_text(),
                "price": Manager.extract_price(product_price_new.get_text())
                if product_price_new
                else Manager.extract_price(product_price_old.get_text()),
                "offer": Manager.extract_price(product_price_new.get_text())
                if not product_price_new
                else None,
                "parse_date": parse_date,
                "category": str(page.get("category")),
                "sub_category": str(page.get("sub_category")).lower(),
                "shop_name": "cifrus",
            }
            result.append(product)
    return result


def parse_cifrus():
    """
    Main func to obtain all items from cifrus.ru web-site

    Returns:
        list[dict]: parsed product's cards of all items that exists in shop
    """
    # initally get left-side menu items with their sub_items values
    # into one huge list of urls like [{ href : '...', category : '[main_name]', sub_category : '[sub_item_name]''}]
    hrefs = list()
    # get list of dropdown menu items
    menu = get_menu(URL)
    # we dedicate that every page from menu have an additional one in their content
    # and we must to test it and get sub_menu arrays from every page
    sub_pages = fetch_concurrent_thread(menu)
    # iter by fetched items and get href of every subcategory_item to result list
    extracted_sub_hrefs = [get_true_hrefs(item) for item in sub_pages]
    # extract list's from list into new single one
    hrefs = array_spread(extracted_sub_hrefs)
    # get content from all pages from ``href`` list
    pages = fetch_concurrent_thread(hrefs)
    # parse product's categories into result list
    data = [parse_card(item) for item in pages]
    # another one spread list of lists into single result one
    data = array_spread(data)
    return data


if __name__ == "__main__":
    start = time.time()
    parse_cifrus()
    print(f"Done for {time.time() - start } ")
