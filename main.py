import asyncio
import time
import pandas as pd
from src.cifrus import parse_cifrus
from src.gastore import parse_gastore
from src.helper import Manager
from src.spar import parse_spar


async def main():

    spar, gastore, cifrus = await asyncio.gather(*[parse_spar(),parse_gastore(), parse_cifrus()])

    columns = [
        "name",
        "price",
        "offer",
        "shop_name",
        "category",
        "sub_category",
        "parse_date", 
    ]

    cifrus_df = pd.DataFrame(cifrus, columns=columns)
    spar_df = pd.DataFrame(spar, columns=columns)
    gastore_df = pd.DataFrame(gastore, columns=columns)
    
    Manager.save_to_file(spar, "spar")
    Manager.save_to_file(gastore, "gastore")
    Manager.save_to_file(cifrus, "cifrus")

    df = pd.concat([spar_df, gastore_df, cifrus_df])

    Manager.to_click(df)


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    print(f"Done for {time.time() - start} ")
