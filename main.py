import asyncio
from multiprocessing import Process
import time
import pandas as pd
from cifrus import parse_cifrus
from gastore import parse_gastore
from helper import Manager, get_results, run_async_function
from spar import parse_spar


async def main():

    spar = await parse_spar()
    gastore = await parse_gastore()
    cifrus = await parse_cifrus()

    columns=  ['name', 'price', 'offer', 'shopName', 'category', 'sub_category', 'parse_date' ]

    spar_df = pd.DataFrame(spar, columns=columns)
    gastore_df = pd.DataFrame(gastore, columns=columns)
    cifrus_df = pd.DataFrame(cifrus, columns=columns)

    df = pd.concat([spar_df, gastore_df, cifrus_df])

    print(df.head(5))
    print(len(df))

    Manager.save_to_file(spar, 'spar')
    Manager.save_to_file(gastore, 'gastore')
    Manager.save_to_file(cifrus, 'cifrus')
 
    Manager.to_click(df)


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    print(f'Done for {time.time() - start } ')