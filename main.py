from concurrent.futures import ProcessPoolExecutor
import time
import pandas as pd
from src.cifrus import parse_cifrus
from src.gastore import parse_gastore
from src.helper import Manager, array_spread, make_executable
from src.spar import parse_spar
from config import result_columns

# TODO  -> add pydantic !!!!!!!!!!!!!!!!!!!!!!!!
def main():

    cf = parse_cifrus()
    # print(cf)
    Manager.save_to_file(cf, "all_in_one")
    # with ProcessPoolExecutor(3) as w:
    #     res = w.map(make_executable, [parse_gastore, parse_spar, parse_cifrus])
    #
    # list_results = list(res)
    #
    # spreaded_results = array_spread(list_results)
    #
    # df = pd.DataFrame(spreaded_results, columns=result_columns)
    #
    # Manager.save_to_file(array_spread(spreaded_results), "all_in_one")
    #
    # # Manager.to_click(df)


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Done for {time.time() - start} ")
