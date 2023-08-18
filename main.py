from concurrent.futures import ProcessPoolExecutor
import time
import pandas as pd
from src.cifrus import parse_cifrus
from src.gastore import parse_gastore
from src.helper import Manager, array_spread
from src.spar import parse_spar

columns = [
    "name",
    "price",
    "offer",
    "shop_name",
    "category",
    "sub_category",
    "parse_date",
]


def make_executable(func):
    return func()


def main():
    with ProcessPoolExecutor(3) as w:
        res = w.map(make_executable, [parse_gastore, parse_spar, parse_cifrus])

    list_results = list(res)

    spreaded_results = array_spread(list_results)

    df = pd.DataFrame(spreaded_results, columns=columns)

    Manager.save_to_file(array_spread(spreaded_results), "all_in_one")

    # Manager.to_click(df)


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Done for {time.time() - start} ")
