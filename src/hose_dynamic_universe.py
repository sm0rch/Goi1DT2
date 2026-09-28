"""
HOSE Dynamic Universe Pipeline
================================
Chiến lược:
1. Lấy danh sách ~300 mã HOSE có vốn hóa lớn nhất (đủ bao quát top-100 mọi kỳ)
2. Kéo OHLCV daily (checkpoint → skip mã đã có)
3. Kéo lịch sử cổ phiếu lưu hành (capital_history)
4. Tính market cap từng tháng → lọc top-100 mỗi tháng → dynamic universe
5. Kéo book value + income statement cho các mã trong universe
6. Build monthly_panel_hose.parquet
"""

import pandas as pd
import numpy as np
import time
import logging
from pathlib import Path
from datetime import datetime, date

from vnstock import Listing
from vnstock.api.quote import Quote
from vnstock.api.company import Company
from vnstock.api.financial import Finance

# ─── Config ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/hose_pipeline.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("hose")

ROOT_DIR = Path("/Users/wuocnguyen/Goi1DT2")
RAW_DIR  = ROOT_DIR / "data" / "raw"
CLEAN_DIR= ROOT_DIR / "data" / "clean"
CKPT_DIR = RAW_DIR / "checkpoints_hose"
CKPT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "2018-01-01"
END_DATE   = date.today().strftime("%Y-%m-%d")
SLEEP_PRICE   = 1.5
SLEEP_FINANCE = 1.2
MAX_RETRIES   = 3
TOP_N = 100          # số mã lọc mỗi kỳ
UNIVERSE_SIZE = 300  # kéo top N này để đảm bảo cover top-100 mọi kỳ


# ─── Step 0: HOSE candidate universe ────────────────────────────────────────
def get_hose_candidates() -> list[str]:
    """Lấy ~300 mã cổ phiếu HOSE có vốn hóa lớn nhất hiện tại."""
    log.info("Lấy danh sách cổ phiếu HOSE...")
    hose = Listing("KBS").symbols_by_exchange("HOSE")
    stocks = hose[hose['type'] == 'stock'][['symbol', 'ceiling']].copy()
    stocks = stocks[stocks['ceiling'] >= 5000].dropna()  # loại penny stocks < 5k

    # Ưu tiên mã có giá cao (proxy cho vốn hóa lớn ở bước sàng lọc đầu)
    # Lấy thêm buffer 50 mã để đảm bảo đủ
    stocks = stocks.sort_values('ceiling', ascending=False).head(UNIVERSE_SIZE + 50)
    syms = stocks['symbol'].tolist()
    log.info(f"HOSE candidates: {len(syms)} mã")
    pd.DataFrame({'symbol': syms}).to_csv(RAW_DIR / "hose_candidates.csv", index=False)
    return syms


# ─── Step 1: OHLCV với checkpoint ───────────────────────────────────────────
def fetch_price_vci(symbol: str) -> pd.DataFrame | None:
    for attempt in range(MAX_RETRIES):
        try:
            q = Quote(symbol=symbol, source="VCI")
            df = q.history(start=START_DATE, end=END_DATE, interval="1D")
            if df is None or df.empty:
                return None
            df = df.rename(columns={"time": "date"})
            df["date"] = pd.to_datetime(df["date"])
            df["symbol"] = symbol
            return df[["symbol", "date", "open", "high", "low", "close", "volume"]]
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(SLEEP_PRICE * 2)
            else:
                log.warning(f"✗ {symbol}: {e}")
    return None


def fetch_price_with_checkpoint(symbol: str, idx: int, total: int) -> pd.DataFrame | None:
    ckpt = CKPT_DIR / f"price_{symbol}.parquet"
    if ckpt.exists():
        df = pd.read_parquet(ckpt)
        df["date"] = pd.to_datetime(df["date"])
        last = df["date"].max()
        if (pd.Timestamp(END_DATE) - last).days <= 5:
            log.info(f"  [{idx:4d}/{total}] ↩ {symbol:6s}: checkpoint OK ({len(df):,} rows)")
            return df
    df = fetch_price_vci(symbol)
    if df is not None and not df.empty:
        df.to_parquet(ckpt, index=False)
        log.info(f"  [{idx:4d}/{total}] ✓ {symbol:6s}: {len(df):,} rows")
    return df


# ─── Step 2: Capital History ─────────────────────────────────────────────────
def fetch_cap_hist(symbol: str, idx: int, total: int) -> pd.DataFrame | None:
    try:
        c = Company(symbol=symbol, source="KBS")
        df = c.capital_history()
        if df is not None and not df.empty:
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, format="mixed", errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date")
            df["shares"] = df["charter_capital"].astype(float) / 10000.0
            df["symbol"] = symbol
            return df[["symbol", "date", "shares"]]
    except:
        pass
    # Fallback: snapshot
    try:
        ov = Company(symbol=symbol, source="VCI").overview()
        shares = float(ov["issue_share"].iloc[0])
        return pd.DataFrame([{"symbol": symbol, "date": pd.Timestamp("2000-01-01"), "shares": shares}])
    except:
        pass
    log.warning(f"  [{idx:4d}/{total}] ✗ {symbol:6s}: cap_hist failed")
    return None


# ─── Step 3: Dynamic top-100 selection ──────────────────────────────────────
def build_dynamic_universe(price_df: pd.DataFrame, cap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Mỗi tháng, lọc top-100 mã có market cap lớn nhất.
    Trả về DataFrame với cột [symbol, date] đánh dấu mã nào được include trong kỳ đó.
    """
    log.info("Tính dynamic universe top-100 theo từng tháng...")
    price_df = price_df.copy()
    price_df["date"] = pd.to_datetime(price_df["date"])

    def _resample(grp):
        g = grp.set_index("date").resample("ME").agg(close_eom=("close", "last"))
        g["symbol"] = grp["symbol"].iloc[0]
        return g.reset_index()

    monthly = price_df.groupby("symbol", group_keys=False).apply(_resample).reset_index(drop=True)

    cap_df = cap_df.copy()
    cap_df["date"] = pd.to_datetime(cap_df["date"])
    cap_df = cap_df.sort_values("date")
    monthly = monthly.sort_values("date")

    merged = pd.merge_asof(monthly, cap_df, by="symbol", on="date", direction="backward")
    merged["market_cap"] = merged["close_eom"] * merged["shares"]

    # Mỗi tháng: rank và lấy top-100
    def _top100(grp):
        ranked = grp.dropna(subset=["market_cap"]).nlargest(TOP_N, "market_cap")
        return ranked[["symbol", "date"]]

    universe = merged.groupby("date", group_keys=False).apply(_top100).reset_index(drop=True)
    log.info(f"Dynamic universe: {universe['symbol'].nunique()} unique symbols across {universe['date'].nunique()} months")
    return universe, merged


# ─── Step 4: Financial data ──────────────────────────────────────────────────
def get_row_value(df, en_re, vi_re, yr):
    mask = df["item_en"].str.upper().str.contains(en_re, na=False)
    if not mask.any() and vi_re:
        mask = df["item"].str.upper().str.contains(vi_re, na=False)
    if not mask.any():
        return None
    matched = df[mask]
    yr_cols = [c for c in df.columns if str(c).isdigit()]
    if len(matched) > 1 and yr_cols:
        matched = matched.sort_values(str(max(int(c) for c in yr_cols)), ascending=False)
    try:
        return float(matched.iloc[0].get(str(yr)))
    except:
        return None


def fetch_financials(symbol: str, idx: int, total: int) -> pd.DataFrame | None:
    for attempt in range(MAX_RETRIES):
        try:
            f   = Finance(symbol=symbol, source="VCI", period="year")
            bs  = f.balance_sheet(lang="en")
            ins = f.income_statement(lang="en")
            if bs is None or bs.empty:
                return None

            yr_cols = [c for c in bs.columns if str(c).isdigit()]
            recs = []
            for yr in yr_cols:
                be = get_row_value(bs, "OWNER'S EQUITY|TOTAL EQUITY|SHAREHOLDERS", "VỐN CHỦ SỞ HỮU", yr)
                ta = get_row_value(bs, "TOTAL ASSETS", "TỔNG CỘNG TÀI SẢN", yr)
                sales   = get_row_value(ins, "NET SALES", "DOANH THU THUẦN", yr) if ins is not None else None
                cogs    = get_row_value(ins, "COST OF SALES", "GIÁ VỐN HÀNG BÁN", yr) if ins is not None else None
                sga1    = get_row_value(ins, "SELLING EXPENSE", "CHI PHÍ BÁN HÀNG", yr) if ins is not None else None
                sga2    = get_row_value(ins, "GENERAL AND ADMIN", "CHI PHÍ QUẢN LÝ", yr) if ins is not None else None
                int_exp = get_row_value(ins, "INTEREST EXPENSE", "CHI PHÍ LÃI VAY", yr) if ins is not None else None
                recs.append({
                    "symbol": symbol, "year": int(yr),
                    "book_equity": be, "total_assets": ta,
                    "net_sales": sales, "cogs": cogs,
                    "selling_exp": sga1, "admin_exp": sga2, "interest_exp": int_exp
                })
            log.info(f"  [{idx:4d}/{total}] ✓ Fin {symbol:6s}: {len(yr_cols)} yrs")
            return pd.DataFrame(recs)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(SLEEP_FINANCE * 2)
            else:
                log.warning(f"  [{idx:4d}/{total}] ✗ Fin {symbol:6s}: {e}")
    return None


# ─── Step 5: Build panel ─────────────────────────────────────────────────────
def build_panel(price_df, cap_df, fin_df, universe_df, monthly_mktcap_df):
    log.info("Building final monthly panel...")
    price_df = price_df.copy()
    price_df["date"] = pd.to_datetime(price_df["date"])

    def _resample(grp):
        g = grp.set_index("date").resample("ME").agg(
            close_eom=("close", "last"),
            volume_avg=("volume", "mean"),
        )
        g["monthly_return"] = np.log(g["close_eom"] / g["close_eom"].shift(1))
        g["symbol"] = grp["symbol"].iloc[0]
        return g.reset_index()

    monthly = price_df.groupby("symbol", group_keys=False).apply(_resample).reset_index(drop=True)

    # Merge market cap (already computed in monthly_mktcap_df)
    monthly = monthly.merge(
        monthly_mktcap_df[["symbol", "date", "market_cap", "shares"]],
        on=["symbol", "date"], how="left"
    )
    monthly = monthly.rename(columns={"market_cap": "market_cap_eom", "shares": "shares_outstanding"})

    # Filter: only keep rows that are in the dynamic universe
    universe_df["in_universe"] = True
    monthly = monthly.merge(universe_df, on=["symbol", "date"], how="left")
    monthly = monthly[monthly["in_universe"] == True].copy()

    # Point-in-time financials
    monthly["year"]  = monthly["date"].dt.year
    monthly["month"] = monthly["date"].dt.month
    monthly["ff_year"] = np.where(monthly["month"] >= 7, monthly["year"], monthly["year"] - 1)

    fin_pit = fin_df.copy()
    fin_pit["ff_year"] = fin_pit["year"] + 1

    def safe_op(row):
        cols = ["cogs", "selling_exp", "admin_exp", "interest_exp"]
        exp_sum = sum(
            row[c] if pd.notna(row[c]) and row[c] < 0 else (-row[c] if pd.notna(row[c]) else 0)
            for c in cols
        )
        sales = row["net_sales"] if pd.notna(row["net_sales"]) else 0
        return sales + exp_sum

    fin_pit["OP_num"] = fin_pit.apply(safe_op, axis=1)
    fin_pit["OP"] = fin_pit["OP_num"] / fin_pit["book_equity"]
    fin_pit = fin_pit.sort_values(["symbol", "year"])
    fin_pit["ta_prev"] = fin_pit.groupby("symbol")["total_assets"].shift(1)
    fin_pit["Inv"] = (fin_pit["total_assets"] - fin_pit["ta_prev"]) / fin_pit["ta_prev"]

    panel = monthly.merge(
        fin_pit[["symbol", "ff_year", "book_equity", "OP", "Inv", "total_assets"]],
        on=["symbol", "ff_year"], how="left"
    )
    panel["book_to_market"] = panel["book_equity"] / panel["market_cap_eom"]
    panel = panel.dropna(subset=["monthly_return", "close_eom"])
    panel = panel[panel["monthly_return"].abs() <= 1.5].copy()
    panel["risk_free_rate"] = "TO_BE_ADDED_EXTERNALLY"
    return panel


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    SEP = "=" * 65
    print(SEP)
    print("  HOSE Dynamic Universe Pipeline")
    print(f"  Period: {START_DATE} → {END_DATE}")
    print(f"  Universe: top-{TOP_N} by market cap per month (from {UNIVERSE_SIZE}+ candidates)")
    print(SEP)

    # ── 0. Candidates ──────────────────────────────────────────────────────────
    candidates = get_hose_candidates()
    N = len(candidates)

    # ── 1. OHLCV ──────────────────────────────────────────────────────────────
    log.info(f"\n📈 [1/4] Pull OHLCV cho {N} HOSE candidates...")
    all_prices, failed_price = [], []
    for i, sym in enumerate(candidates, 1):
        df = fetch_price_with_checkpoint(sym, i, N)
        if df is not None and not df.empty:
            all_prices.append(df)
        else:
            failed_price.append(sym)
        if i < N:
            time.sleep(SLEEP_PRICE)

    price_df = pd.concat(all_prices).sort_values(["symbol", "date"]).reset_index(drop=True)
    price_df.to_parquet(RAW_DIR / "hose_ohlcv_raw.parquet", index=False)
    log.info(f"✅ hose_ohlcv_raw.parquet: {price_df['symbol'].nunique()} mã, {len(price_df):,} rows | Failed: {failed_price or 'none'}")

    # ── 2. Capital History ─────────────────────────────────────────────────────
    log.info(f"\n💰 [2/4] Pull capital history...")
    existing_cap = []
    cap_recs = []
    if (RAW_DIR / "hose_capital_history.csv").exists():
        ex = pd.read_csv(RAW_DIR / "hose_capital_history.csv")
        existing_cap = ex["symbol"].unique().tolist()
        cap_recs.append(ex)
        log.info(f"  Loaded {len(existing_cap)} from existing hose_capital_history.csv")

    for i, sym in enumerate(candidates, 1):
        if sym in existing_cap:
            continue
        df = fetch_cap_hist(sym, i, N)
        if df is not None:
            cap_recs.append(df)
        time.sleep(1.2)

    cap_df = pd.concat(cap_recs).dropna(subset=["date"])
    cap_df["date"] = pd.to_datetime(cap_df["date"])
    cap_df.to_csv(RAW_DIR / "hose_capital_history.csv", index=False)
    log.info(f"✅ hose_capital_history.csv: {cap_df['symbol'].nunique()} mã")

    # ── 3. Dynamic universe ────────────────────────────────────────────────────
    log.info(f"\n🎯 [3/4] Build dynamic top-{TOP_N} universe...")
    universe_df, monthly_mktcap_df = build_dynamic_universe(price_df, cap_df)
    universe_symbols = universe_df["symbol"].unique().tolist()
    log.info(f"  Unique symbols ever in top-{TOP_N}: {len(universe_symbols)}")
    universe_df.to_csv(RAW_DIR / "hose_dynamic_universe.csv", index=False)

    # ── 4. Financials for universe symbols ─────────────────────────────────────
    log.info(f"\n📚 [4/4] Pull financials cho {len(universe_symbols)} mã...")
    existing_fin = []
    fin_recs = []
    if (RAW_DIR / "hose_financials_annual.csv").exists():
        ex = pd.read_csv(RAW_DIR / "hose_financials_annual.csv")
        existing_fin = ex["symbol"].unique().tolist()
        fin_recs.append(ex)
        log.info(f"  Loaded {len(existing_fin)} from existing hose_financials_annual.csv")

    for i, sym in enumerate(universe_symbols, 1):
        if sym in existing_fin:
            continue
        df = fetch_financials(sym, i, len(universe_symbols))
        if df is not None:
            fin_recs.append(df)
        time.sleep(SLEEP_FINANCE)

    fin_df = pd.concat(fin_recs).sort_values(["symbol", "year"]).reset_index(drop=True)
    fin_df.to_csv(RAW_DIR / "hose_financials_annual.csv", index=False)
    log.info(f"✅ hose_financials_annual.csv: {fin_df['symbol'].nunique()} mã")

    # ── 5. Build panel ─────────────────────────────────────────────────────────
    panel = build_panel(price_df, cap_df, fin_df, universe_df, monthly_mktcap_df)
    panel.to_parquet(CLEAN_DIR / "monthly_panel_hose_top100.parquet", index=False)
    panel.to_csv(CLEAN_DIR / "monthly_panel_hose_top100.csv", index=False)

    print(f"\n{SEP}")
    print("  ✅ HOSE DYNAMIC UNIVERSE PIPELINE COMPLETE")
    print(f"  Unique symbols (ever top-{TOP_N}): {panel['symbol'].nunique()}")
    print(f"  Date range : {panel['date'].min().date()} → {panel['date'].max().date()}")
    print(f"  Total rows : {len(panel):,}")
    bm = panel["book_to_market"].isna()
    print(f"  Missing B/M: {bm.sum():,} ({bm.mean()*100:.1f}%)")
    print(SEP)

    files = [
        RAW_DIR / "hose_candidates.csv",
        RAW_DIR / "hose_ohlcv_raw.parquet",
        RAW_DIR / "hose_capital_history.csv",
        RAW_DIR / "hose_dynamic_universe.csv",
        RAW_DIR / "hose_financials_annual.csv",
        CLEAN_DIR / "monthly_panel_hose_top100.parquet",
        CLEAN_DIR / "monthly_panel_hose_top100.csv",
    ]
    print("\n📁 Output files:")
    for f in files:
        size = f"{f.stat().st_size/1024:.0f} KB" if f.exists() else "not found"
        mark = "← MAIN OUTPUT" if "monthly_panel_hose_top100.parquet" in f.name else ""
        print(f"   {f.name}  [{size}] {mark}")


if __name__ == "__main__":
    main()
