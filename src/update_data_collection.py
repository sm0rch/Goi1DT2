import pandas as pd
import numpy as np
import time
import logging
from pathlib import Path
from datetime import datetime

# vnstock modules
from vnstock.api.quote import Quote
from vnstock.api.financial import Finance
from vnstock.api.company import Company
from vnstock import Listing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ff5_data")

ROOT_DIR = Path("/Users/wuocnguyen/Goi1DT2")
RAW_DIR = ROOT_DIR / "data" / "raw"
CLEAN_DIR = ROOT_DIR / "data" / "clean"
CKPT_DIR = RAW_DIR / "checkpoints"

SLEEP_FINANCE = 1.0

def get_vn100_symbols():
    listing = Listing("KBS")
    df = listing.symbols_by_group("VN100")
    if isinstance(df, pd.Series): return df.tolist()
    if "symbol" in df.columns: return df["symbol"].tolist()
    if "ticker" in df.columns: return df["ticker"].tolist()
    return []

def get_row_value(bs_df, en_regex, vi_regex, year):
    # Try item_en first
    mask = bs_df["item_en"].str.upper().str.contains(en_regex, na=False)
    if not mask.any() and vi_regex:
        mask = bs_df["item"].str.upper().str.contains(vi_regex, na=False)
    if not mask.any():
        return None
    matched = bs_df[mask]
    year_cols = [c for c in bs_df.columns if str(c).isdigit()]
    if len(matched) > 1 and year_cols:
        latest_yr = str(max(int(c) for c in year_cols))
        matched = matched.sort_values(latest_yr, ascending=False)
    try:
        return float(matched.iloc[0].get(str(year)))
    except:
        return None

def fetch_financials(symbol: str, idx: int, total: int):
    for attempt in range(3):
        try:
            f = Finance(symbol=symbol, source="VCI", period="year")
            bs = f.balance_sheet(lang="en")
            ins = f.income_statement(lang="en")
            if bs is None or bs.empty:
                return None
            
            year_cols = [c for c in bs.columns if str(c).isdigit()]
            records = []
            for yr in year_cols:
                # Book Equity
                be = get_row_value(bs, "OWNER'S EQUITY|TOTAL EQUITY|SHAREHOLDERS", "VỐN CHỦ SỞ HỮU", yr)
                if be is None:
                    be = get_row_value(bs, "XXXXX", "TỔNG CỘNG NGUỒN VỐN", yr)
                
                # Total Assets
                ta = get_row_value(bs, "TOTAL ASSETS", "TỔNG CỘNG TÀI SẢN", yr)
                
                # Net Sales
                sales = None
                if ins is not None and not ins.empty:
                    sales = get_row_value(ins, "NET SALES", "DOANH THU THUẦN", yr)
                    if sales is None: sales = get_row_value(ins, "SALES", "DOANH THU BÁN HÀNG", yr)
                    
                    cogs = get_row_value(ins, "COST OF SALES", "GIÁ VỐN HÀNG BÁN", yr)
                    sga1 = get_row_value(ins, "SELLING EXPENSE", "CHI PHÍ BÁN HÀNG", yr)
                    sga2 = get_row_value(ins, "GENERAL AND ADMIN", "CHI PHÍ QUẢN LÝ", yr)
                    int_exp = get_row_value(ins, "INTEREST EXPENSE", "CHI PHÍ LÃI VAY", yr)
                else:
                    sales, cogs, sga1, sga2, int_exp = None, None, None, None, None
                
                records.append({
                    "symbol": symbol, "year": int(yr),
                    "book_equity": be,
                    "total_assets": ta,
                    "net_sales": sales,
                    "cogs": cogs,
                    "selling_exp": sga1,
                    "admin_exp": sga2,
                    "interest_exp": int_exp
                })
            log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s} Fin: {len(year_cols)} yrs")
            return pd.DataFrame(records)
        except Exception as e:
            if attempt == 2: log.warning(f"  [{idx:3d}/{total}] ✗ {symbol:6s}: {e}")
            time.sleep(1.0)
    return None

def fetch_shares_history(symbol: str, idx: int, total: int):
    # Try KBS historical
    for attempt in range(2):
        try:
            c = Company(symbol=symbol, source="KBS")
            cap_hist = c.capital_history()
            if cap_hist is not None and not cap_hist.empty:
                cap_hist["date"] = pd.to_datetime(cap_hist["date"])
                cap_hist = cap_hist.sort_values("date")
                cap_hist["shares"] = cap_hist["charter_capital"].astype(float) / 10000.0
                log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s} CapHist: {len(cap_hist)} events")
                cap_hist["symbol"] = symbol
                return cap_hist[["symbol", "date", "shares"]]
        except Exception as e:
            if "Rate limit" in str(e):
                log.warning(f"  [{idx:3d}/{total}] RATE LIMIT on CapHist. Waiting...")
                time.sleep(60)
            pass
            
    # Fallback to snapshot from overview
    try:
        c = Company(symbol=symbol, source="VCI")
        ov = c.overview()
        shares = float(ov["issue_share"].iloc[0])
        log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s} CapHist: SNAPSHOT fallback")
        return pd.DataFrame([{"symbol": symbol, "date": pd.to_datetime("2000-01-01"), "shares": shares}])
    except Exception as e:
        log.warning(f"  [{idx:3d}/{total}] ✗ {symbol:6s} CapHist failed: {e}")
    return None

def main():
    symbols = get_vn100_symbols()
    N = len(symbols)
    
    # 1. Price is already downloaded in ohlcv_raw.parquet
    price_df = pd.read_parquet(RAW_DIR / "ohlcv_raw.parquet")
    
    # 2. Capital History
    log.info("Fetching Historical Shares...")
    cap_records = []
    
    # Resume cap_df if exists
    existing_cap = []
    if (RAW_DIR / "capital_history.csv").exists():
        existing_cap_df = pd.read_csv(RAW_DIR / "capital_history.csv")
        existing_cap = existing_cap_df["symbol"].unique().tolist()
        cap_records.append(existing_cap_df)
        log.info(f"Loaded {len(existing_cap)} symbols from existing capital_history.csv")
        
    for i, sym in enumerate(symbols, 1):
        if sym in existing_cap: continue
        df = fetch_shares_history(sym, i, N)
        if df is not None: cap_records.append(df)
        time.sleep(1.5) # avoid KBS rate limit
    cap_df = pd.concat(cap_records)
    cap_df.to_csv(RAW_DIR / "capital_history.csv", index=False)
    
    # 3. Financials
    log.info("Fetching Financials (Assets, OP components)...")
    fin_records = []
    
    existing_fin = []
    if (RAW_DIR / "financials_annual.csv").exists():
        existing_fin_df = pd.read_csv(RAW_DIR / "financials_annual.csv")
        existing_fin = existing_fin_df["symbol"].unique().tolist()
        fin_records.append(existing_fin_df)
        log.info(f"Loaded {len(existing_fin)} symbols from existing financials_annual.csv")
        
    for i, sym in enumerate(symbols, 1):
        if sym in existing_fin: continue
        df = fetch_financials(sym, i, N)
        if df is not None: fin_records.append(df)
        time.sleep(1.5)
    fin_df = pd.concat(fin_records)
    fin_df.to_csv(RAW_DIR / "financials_annual.csv", index=False)
    
    # 4. Build Monthly Panel
    log.info("Building Monthly Panel with FF5 variables...")
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
    
    # Merge shares historical ASOF
    cap_df["date"] = pd.to_datetime(cap_df["date"])
    cap_df = cap_df.sort_values("date")
    
    # AsOf merge requires sorted dates on both sides
    monthly = monthly.sort_values("date")
    merged = pd.merge_asof(
        monthly, 
        cap_df, 
        by="symbol", 
        on="date", 
        direction="backward"
    )
    merged["shares_outstanding"] = merged["shares"]
    merged["market_cap_eom"] = merged["close_eom"] * merged["shares_outstanding"]
    
    # Financials point-in-time
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month
    
    # FF rules: portfolio formed in July T uses T-1 accounting data
    # month >= 7 -> ff_year = year, month < 7 -> ff_year = year - 1
    merged["ff_year"] = np.where(merged["month"] >= 7, merged["year"], merged["year"] - 1)
    
    fin_pit = fin_df.copy()
    fin_pit["ff_year"] = fin_pit["year"] + 1 # Accounting year T is used for ff_year T+1
    
    # Calculate OP and Inv
    # OP = (Revenues - COGS - SG&A - Interest) / Book Equity_t
    # In vnstock, expenses are usually negative, so Revenue + COGS + SG&A1 + SG&A2 + Interest
    # Let's ensure they are added properly if negative
    def safe_add(row):
        cols = ["cogs", "selling_exp", "admin_exp", "interest_exp"]
        expenses = sum(row[c] if pd.notna(row[c]) and row[c] < 0 else (-(row[c]) if pd.notna(row[c]) else 0) for c in cols)
        sales = row["net_sales"] if pd.notna(row["net_sales"]) else 0
        return sales + expenses # since expenses is negative sum
        
    fin_pit["OP_num"] = fin_pit.apply(safe_add, axis=1)
    fin_pit["OP"] = fin_pit["OP_num"] / fin_pit["book_equity"]
    
    # Inv = (TA_t - TA_t-1) / TA_t-1
    fin_pit = fin_pit.sort_values(["symbol", "year"])
    fin_pit["total_assets_prev"] = fin_pit.groupby("symbol")["total_assets"].shift(1)
    fin_pit["Inv"] = (fin_pit["total_assets"] - fin_pit["total_assets_prev"]) / fin_pit["total_assets_prev"]
    
    # Merge financials
    final_panel = merged.merge(
        fin_pit[["symbol", "ff_year", "book_equity", "OP", "Inv", "total_assets"]],
        on=["symbol", "ff_year"],
        how="left"
    )
    final_panel["book_to_market"] = final_panel["book_equity"] / final_panel["market_cap_eom"]
    
    final_panel = final_panel.dropna(subset=["monthly_return", "close_eom"])
    final_panel = final_panel[final_panel["monthly_return"].abs() <= 1.5].copy()
    
    # Note for Risk-Free Rate
    final_panel["risk_free_rate"] = "TO_BE_ADDED_EXTERNALLY"
    
    final_panel.to_parquet(CLEAN_DIR / "monthly_panel_ff5.parquet", index=False)
    final_panel.to_csv(CLEAN_DIR / "monthly_panel_ff5.csv", index=False)
    
    print("\n✅ FF5 Panel successfully built: monthly_panel_ff5.parquet")
    print(f"Total rows: {len(final_panel)}")

if __name__ == "__main__":
    main()
