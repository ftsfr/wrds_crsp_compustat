"""
This module contains functions to load the datasets.
It is used to load the datasets into memory.

List of datasets:

- CRSP_monthly_stock_ret: CRSP stock returns
- CRSP_monthly_stock_retx: CRSP stock returns (without dividends)

"""

import calc_Fama_French_1993
import pull_CRSP_Compustat

import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"


## CRSP_monthly_stock_ret
df_all = pull_CRSP_Compustat.load_CRSP_stock_ciz(data_dir=DATA_DIR)
df_all = calc_Fama_French_1993.subset_CRSP_to_common_stock_and_exchanges(df_all)
df_all = df_all.sort_values(by=["permno", "mthcaldt"])

df = df_all[["permno", "mthcaldt", "mthret"]]
# df_all.info()
df = df.rename(columns={"permno": "unique_id", "mthcaldt": "ds", "mthret": "y"})
df = df.dropna()
df = df.reset_index(drop=True)
df.to_parquet(DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet")


## CRSP_monthly_stock_retx
df = df_all[["permno", "mthcaldt", "mthretx"]]
# df_all.info()
df = df.rename(columns={"permno": "unique_id", "mthcaldt": "ds", "mthretx": "y"})
df = df.dropna()
df = df.reset_index(drop=True)
df.to_parquet(DATA_DIR / "ftsfr_CRSP_monthly_stock_retx.parquet")
