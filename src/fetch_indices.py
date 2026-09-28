import pandas as pd
from vnstock.api.quote import Quote
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

try:
    log = logging.getLogger("fetch_indices")
    df_vni = Quote(symbol="VNINDEX", source="VCI").history(start="2018-01-01", end="2026-09-28")
    df_vni["symbol"] = "VNINDEX"
    
    df_vn100 = Quote(symbol="VN100", source="VCI").history(start="2018-01-01", end="2026-09-28")
    df_vn100["symbol"] = "VN100"
    
    df = pd.concat([df_vni, df_vn100], ignore_index=True)
    df.to_csv("data/raw/index_history.csv", index=False)
    log.info(f"Saved {len(df)} rows to data/raw/index_history.csv")
except Exception as e:
    log.error(f"Error fetching indices: {e}")
