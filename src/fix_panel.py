import pandas as pd
import numpy as np
import time
import logging
from pathlib import Path

from vnstock.api.company import Company

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fix")

ROOT_DIR = Path("/Users/wuocnguyen/Goi1DT2")
RAW_DIR = ROOT_DIR / "data" / "raw"
CLEAN_DIR = ROOT_DIR / "data" / "clean"

def get_cap_hist(symbol):
    try:
        c = Company(symbol=symbol, source="KBS")
        df = c.capital_history()
        if df is not None and not df.empty:
            # Fix date parsing! KBS returns YYYY-MM-DD or DD/MM/YYYY? 
            # It seems sometimes it's %d/%m/%Y. Let's use format='mixed', dayfirst=True
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, format="mixed", errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date")
            df["shares"] = df["charter_capital"].astype(float) / 10000.0
            df["symbol"] = symbol
            return df[["symbol", "date", "shares"]]
    except: pass
    
    try:
        c = Company(symbol=symbol, source="VCI")
        ov = c.overview()
        shares = float(ov["issue_share"].iloc[0])
        return pd.DataFrame([{"symbol": symbol, "date": pd.to_datetime("2000-01-01"), "shares": shares}])
    except: pass
    return None

def main():
    symbols = pd.read_csv(RAW_DIR / "vn100_symbols.csv")["symbol"].tolist()
    
    log.info("Refetching correct capital history...")
    recs = []
    for i, sym in enumerate(symbols):
        df = get_cap_hist(sym)
        if df is not None: recs.append(df)
        time.sleep(1.2)
    cap_df = pd.concat(recs)
    cap_df = cap_df.dropna(subset=["date"])
    cap_df.to_csv(RAW_DIR / "capital_history.csv", index=False)
    
    # Reload other data
    price_df = pd.read_parquet(RAW_DIR / "ohlcv_raw.parquet")
    fin_df = pd.read_csv(RAW_DIR / "financials_annual.csv")
    
    price_df["date"] = pd.to_datetime(price_df["date"])
    
    def _resample_monthly(grp):
        g = grp.set_index("date").sort_index()
        m = g.resample("ME").agg(
            close_eom=("close", "last"),
            volume_avg=("volume", "mean"),
        )
        m["monthly_return"] = np.log(m["close_eom"] / m["close_eom"].shift(1))
        m.index.name = "date"
        m = m.reset_index()
        m.insert(0, "symbol", grp["symbol"].iloc[0])
        return m

    monthly = price_df.groupby("symbol", group_keys=False).apply(_resample_monthly).reset_index(drop=True)
    
    cap_df["date"] = pd.to_datetime(cap_df["date"])
    cap_df = cap_df.sort_values("date")
    monthly = monthly.sort_values("date")
    
    merged = pd.merge_asof(monthly, cap_df, by="symbol", on="date", direction="backward")
    merged["shares_outstanding"] = merged["shares"]
    merged["market_cap_eom"] = merged["close_eom"] * merged["shares_outstanding"]
    
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month
    merged["ff_year"] = np.where(merged["month"] >= 7, merged["year"], merged["year"] - 1)
    
    fin_pit = fin_df.copy()
    fin_pit["ff_year"] = fin_pit["year"] + 1 
    
    def safe_add(row):
        cols = ["cogs", "selling_exp", "admin_exp", "interest_exp"]
        expenses = sum(row[c] if pd.notna(row[c]) and row[c] < 0 else (-(row[c]) if pd.notna(row[c]) else 0) for c in cols)
        sales = row["net_sales"] if pd.notna(row["net_sales"]) else 0
        return sales + expenses
        
    fin_pit["OP_num"] = fin_pit.apply(safe_add, axis=1)
    fin_pit["OP"] = fin_pit["OP_num"] / fin_pit["book_equity"]
    
    fin_pit = fin_pit.sort_values(["symbol", "year"])
    fin_pit["total_assets_prev"] = fin_pit.groupby("symbol")["total_assets"].shift(1)
    fin_pit["Inv"] = (fin_pit["total_assets"] - fin_pit["total_assets_prev"]) / fin_pit["total_assets_prev"]
    
    final_panel = merged.merge(
        fin_pit[["symbol", "ff_year", "book_equity", "OP", "Inv", "total_assets"]],
        on=["symbol", "ff_year"],
        how="left"
    )
    final_panel["book_to_market"] = final_panel["book_equity"] / final_panel["market_cap_eom"]
    final_panel = final_panel.dropna(subset=["monthly_return", "close_eom"])
    final_panel = final_panel[final_panel["monthly_return"].abs() <= 1.5].copy()
    final_panel["risk_free_rate"] = "TO_BE_ADDED_EXTERNALLY"
    
    final_panel.to_parquet(CLEAN_DIR / "monthly_panel_ff5.parquet", index=False)
    final_panel.to_csv(CLEAN_DIR / "monthly_panel_ff5.csv", index=False)
    log.info(f"Fixed panel! Rows: {len(final_panel)}")

if __name__ == "__main__":
    main()
