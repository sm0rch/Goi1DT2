"""
src/data_collection.py
======================
Thu thập dữ liệu VN100: Giá OHLCV, Market Cap, Book Value

Nguồn dữ liệu (sau khi probe thực tế):
  - Giá OHLCV daily : VCI via vnstock — community limit 8 năm (2018→nay)
                      Sequential, delay 1.5s/mã → tránh timeout
  - Market Cap      : VCI Company.trading_stats()
  - Book Value      : VCI Finance.balance_sheet()

Checkpoint: lưu từng mã → chạy lại sẽ tiếp tục từ mã chưa xong.

Chạy:  python src/data_collection.py
"""

import time
import warnings
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date

warnings.filterwarnings("ignore")

# ── Logging → cả console lẫn file ─────────────────────────────────────────────
log_path = Path(__file__).parent.parent / "data" / "collection_run.log"
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(log_path), mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR       = Path(__file__).parent.parent.resolve()
RAW_DIR        = ROOT_DIR / "data" / "raw"
CLEAN_DIR      = ROOT_DIR / "data" / "clean"
CHECKPOINT_DIR = RAW_DIR / "checkpoints"

for _d in [RAW_DIR, CLEAN_DIR, CHECKPOINT_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
# VCI community = 8 năm tối đa → start từ 2018 để tránh server phải lọc
START_DATE     = "2018-01-01"
END_DATE       = date.today().strftime("%Y-%m-%d")
SLEEP_PRICE    = 1.5   # delay giữa mỗi mã khi pull giá (tránh VCI timeout)
SLEEP_FINANCE  = 1.0   # delay cho market cap / book value
MAX_RETRIES    = 3


# ──────────────────────────────────────────────────────────────────────────────
# 1. VN100 symbols
# ──────────────────────────────────────────────────────────────────────────────

def get_vn100_symbols() -> list[str]:
    from vnstock import Listing
    symbols = sorted(Listing().symbols_by_group("VN100").tolist())
    log.info(f"VN100 universe: {len(symbols)} mã")
    return symbols


# ──────────────────────────────────────────────────────────────────────────────
# 2. Giá OHLCV — VCI sequential
# ──────────────────────────────────────────────────────────────────────────────

def fetch_price_vci(symbol: str) -> pd.DataFrame | None:
    """
    Lấy toàn bộ lịch sử giá daily từ VCI (community: ~8 năm).
    Trả về DataFrame [symbol, date, open, high, low, close, volume].
    """
    from vnstock.api.quote import Quote
    for attempt in range(MAX_RETRIES):
        try:
            q  = Quote(symbol=symbol, source="VCI")
            df = q.history(start=START_DATE, end=END_DATE, interval="1D")
            if df is not None and not df.empty:
                df = df.rename(columns={"time": "date"})
                df["date"]   = pd.to_datetime(df["date"])
                df["symbol"] = symbol
                df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]
                return df.sort_values("date").reset_index(drop=True)
        except Exception as e:
            wait = SLEEP_PRICE * (attempt + 1) * 2
            log.debug(f"  Retry {attempt+1} {symbol}: {e} (wait {wait:.1f}s)")
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)
            else:
                log.warning(f"  ✗ FAIL price {symbol}: {e}")
    return None


def fetch_price_with_checkpoint(symbol: str, idx: int, total: int) -> pd.DataFrame | None:
    """
    Lấy giá với checkpoint: nếu đã có → bỏ qua; nếu thiếu → pull lại.
    """
    cp = CHECKPOINT_DIR / f"price_{symbol}.parquet"

    if cp.exists():
        existing = pd.read_parquet(cp)
        last_date = pd.Timestamp(existing["date"].max())
        end_ts    = pd.Timestamp(END_DATE)
        if last_date >= end_ts - pd.Timedelta(days=5):
            log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s}: checkpoint OK "
                     f"({len(existing):,} rows, đến {last_date.date()})")
            return existing

    df = fetch_price_vci(symbol)
    if df is not None and not df.empty:
        df.to_parquet(cp)
        log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s}: {len(df):,} rows "
                 f"({df['date'].min().date()}→{df['date'].max().date()})")
    else:
        log.warning(f"  [{idx:3d}/{total}] ✗ {symbol:6s}: FAILED")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# 3. Market Cap
# ──────────────────────────────────────────────────────────────────────────────

def fetch_market_cap(symbol: str, idx: int, total: int) -> dict | None:
    from vnstock.api.company import Company
    for attempt in range(MAX_RETRIES):
        try:
            df = Company(symbol=symbol, source="VCI").trading_stats()
            if df is not None and not df.empty:
                row = df.iloc[0]
                cap = row.get("market_cap")
                log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s}: "
                         f"market_cap={cap/1e12:.2f}T" if cap else f"  [{idx}/{total}] ✓ {symbol}")
                return {
                    "symbol":             symbol,
                    "market_cap":         row.get("market_cap"),
                    "shares_outstanding": row.get("number_of_shares_mkt_cap"),
                    "current_price":      row.get("current_price"),
                    "free_float_pct":     row.get("free_float_percentage"),
                }
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(SLEEP_FINANCE * 2)
            else:
                log.warning(f"  [{idx:3d}/{total}] ✗ {symbol:6s}: {e}")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# 4. Book Value
# ──────────────────────────────────────────────────────────────────────────────

def fetch_book_value(symbol: str, idx: int, total: int) -> pd.DataFrame | None:
    from vnstock.api.financial import Finance
    for attempt in range(MAX_RETRIES):
        try:
            f  = Finance(symbol=symbol, source="VCI", period="year")
            bs = f.balance_sheet(lang="en")
            if bs is None or bs.empty:
                return None

            # Tìm dòng "Vốn chủ sở hữu" (Owner's Equity)
            # Ngân hàng dùng chữ HOA: "VỐN CHỦ SỞ HỮU" → dùng case-insensitive
            mask = bs["item"].str.upper().str.contains("VỐN CHỦ SỞ HỮU", na=False)
            if not mask.any():
                # Fallback: tìm trong cột item_en
                mask = bs["item_en"].str.upper().str.contains("OWNER'S EQUITY|TOTAL EQUITY|SHAREHOLDERS", na=False)
            if not mask.any():
                mask = bs["item"].str.contains("Tổng cộng nguồn vốn", na=False)
            if not mask.any():
                return None
            # Lấy dòng đầu tiên khớp (thường là dòng tổng, không phải dòng con)
            # Ưu tiên dòng có giá trị lớn nhất (tổng equity > từng thành phần)
            matched = bs[mask]
            year_cols_tmp = [c for c in bs.columns if str(c).isdigit()]
            if len(matched) > 1 and year_cols_tmp:
                latest_yr = str(max(int(c) for c in year_cols_tmp))
                matched = matched.sort_values(latest_yr, ascending=False)

            row       = matched.iloc[0]
            year_cols = [c for c in bs.columns if str(c).isdigit()]
            records   = []
            for yr in year_cols:
                try:
                    val = float(row.get(yr))
                except (TypeError, ValueError):
                    val = None
                records.append({"symbol": symbol, "year": int(yr), "book_equity": val})

            log.info(f"  [{idx:3d}/{total}] ✓ {symbol:6s}: "
                     f"{len(year_cols)} years ({min(year_cols)}→{max(year_cols)})")
            return pd.DataFrame(records)

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(SLEEP_FINANCE * 2)
            else:
                log.warning(f"  [{idx:3d}/{total}] ✗ {symbol:6s}: {e}")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# 5. Build Monthly Panel (Point-in-Time Fama-French style)
# ──────────────────────────────────────────────────────────────────────────────

def build_monthly_panel(price_df: pd.DataFrame,
                         mktcap_df: pd.DataFrame,
                         bv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Xây dựng bảng panel theo tháng:
      - Lợi suất tháng (log return)
      - Market cap cuối tháng = close_eom × shares_outstanding
      - Book equity (point-in-time: book year T → ff_year T+1, rebalance tháng 7)
      - Book-to-Market ratio
    """
    price_df = price_df.copy()
    price_df["date"] = pd.to_datetime(price_df["date"])
    price_df = price_df.sort_values(["symbol", "date"])

    def _resample_monthly(grp: pd.DataFrame) -> pd.DataFrame:
        g = grp.set_index("date").sort_index()
        m = g.resample("ME").agg(
            close_eom  = ("close",  "last"),
            open_bom   = ("open",   "first"),
            high_month = ("high",   "max"),
            low_month  = ("low",    "min"),
            volume_avg = ("volume", "mean"),
        )
        m["monthly_return"] = np.log(m["close_eom"] / m["close_eom"].shift(1))
        m.index.name = "date"
        m = m.reset_index()
        m.insert(0, "symbol", grp["symbol"].iloc[0])
        return m

    log.info("  Resampling daily → monthly...")
    monthly = (
        price_df
        .groupby("symbol", group_keys=False)
        .apply(_resample_monthly)
        .reset_index(drop=True)
    )

    # Market cap cuối tháng
    shares_map = mktcap_df.set_index("symbol")["shares_outstanding"].to_dict()
    monthly["shares_outstanding"] = monthly["symbol"].map(shares_map)
    monthly["market_cap_eom"]     = monthly["close_eom"] * monthly["shares_outstanding"]

    # Point-in-Time Book Value (Fama-French chuẩn):
    #   book_equity của năm tài chính T (thường công bố trước 30/6/T+1)
    #   → dùng để tính HML cho kỳ tháng 7/T+1 đến tháng 6/T+2
    #   → gán ff_year = T+1
    monthly["year"]    = monthly["date"].dt.year
    monthly["month"]   = monthly["date"].dt.month
    # Tháng 1-6/Y và 7-12/Y đều tra cứu theo ff_year = Y
    # book_equity năm Y-1 đã được shift ff_year = Y (bên dưới)
    monthly["ff_year"] = monthly["year"]

    bv_pit = bv_df.copy()
    bv_pit["ff_year"] = bv_pit["year"] + 1   # shift point-in-time

    monthly = monthly.merge(
        bv_pit[["symbol", "ff_year", "book_equity"]],
        on=["symbol", "ff_year"],
        how="left",
    )
    monthly["book_to_market"] = monthly["book_equity"] / monthly["market_cap_eom"]

    # Làm sạch: bỏ tháng đầu (NaN return) và outlier cực đoan
    monthly = monthly.dropna(subset=["monthly_return", "close_eom"])
    monthly = monthly[monthly["monthly_return"].abs() <= 1.5].copy()

    cols = [
        "symbol", "date", "year", "month", "ff_year",
        "open_bom", "close_eom", "high_month", "low_month", "volume_avg",
        "monthly_return", "shares_outstanding", "market_cap_eom",
        "book_equity", "book_to_market",
    ]
    return monthly[cols].sort_values(["symbol", "date"]).reset_index(drop=True)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    SEP = "=" * 65
    print(SEP)
    print("  VN100 Data Collection — VCI Sequential (stable)")
    print(f"  Period : {START_DATE} → {END_DATE}")
    print(f"  Mode   : 1 luồng, delay {SLEEP_PRICE}s/mã → không rate-limit")
    print(SEP)

    symbols = get_vn100_symbols()
    pd.Series(symbols, name="symbol").to_csv(RAW_DIR / "vn100_symbols.csv", index=False)
    N = len(symbols)

    # ── Step 1: OHLCV Price ────────────────────────────────────────────────────
    log.info(f"\n📈 [1/3] Pull giá OHLCV ({N} mã) ...")
    all_prices   = []
    failed_price = []
    for i, sym in enumerate(symbols, 1):
        df = fetch_price_with_checkpoint(sym, i, N)
        if df is not None and not df.empty:
            all_prices.append(df)
        else:
            failed_price.append(sym)
        if i < N:
            time.sleep(SLEEP_PRICE)

    if not all_prices:
        log.error("❌ Không lấy được dữ liệu giá nào. Kiểm tra kết nối mạng.")
        return

    price_df = pd.concat(all_prices).sort_values(["symbol", "date"]).reset_index(drop=True)
    price_df.to_parquet(RAW_DIR / "ohlcv_raw.parquet", index=False)
    log.info(f"✅ ohlcv_raw.parquet: {price_df['symbol'].nunique()} mã, "
             f"{len(price_df):,} rows | Failed: {failed_price or 'none'}")

    # ── Step 2: Market Cap ─────────────────────────────────────────────────────
    log.info(f"\n💰 [2/3] Pull Market Cap ({N} mã) ...")
    mktcap_records = []
    for i, sym in enumerate(symbols, 1):
        r = fetch_market_cap(sym, i, N)
        if r:
            mktcap_records.append(r)
        if i < N:
            time.sleep(SLEEP_FINANCE)

    mktcap_df = pd.DataFrame(mktcap_records)
    mktcap_df.to_csv(RAW_DIR / "market_cap_snapshot.csv", index=False)
    log.info(f"✅ market_cap_snapshot.csv: {len(mktcap_df)} mã")

    # ── Step 3: Book Value ─────────────────────────────────────────────────────
    log.info(f"\n📚 [3/3] Pull Book Value ({N} mã) ...")
    all_bv   = []
    fail_bv  = []
    for i, sym in enumerate(symbols, 1):
        df = fetch_book_value(sym, i, N)
        if df is not None:
            all_bv.append(df)
        else:
            fail_bv.append(sym)
        if i < N:
            time.sleep(SLEEP_FINANCE)

    bv_df = pd.concat(all_bv).sort_values(["symbol", "year"]).reset_index(drop=True)
    bv_df.to_csv(RAW_DIR / "book_value_annual.csv", index=False)
    log.info(f"✅ book_value_annual.csv: {bv_df['symbol'].nunique()} mã | "
             f"Failed: {fail_bv or 'none'}")

    # ── Build Monthly Panel ────────────────────────────────────────────────────
    log.info("\n🔧 Xây dựng Monthly Panel Dataset ...")
    monthly = build_monthly_panel(price_df, mktcap_df, bv_df)
    monthly.to_parquet(CLEAN_DIR / "monthly_panel.parquet", index=False)
    monthly.to_csv(CLEAN_DIR / "monthly_panel.csv", index=False)

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print("  ✅ DATA COLLECTION COMPLETE")
    print(f"  Symbols    : {monthly['symbol'].nunique()}")
    print(f"  Date range : {monthly['date'].min().date()} → {monthly['date'].max().date()}")
    print(f"  Total rows : {len(monthly):,}")
    bm_miss = monthly["book_to_market"].isna()
    print(f"  Missing B/M: {bm_miss.sum():,} ({bm_miss.mean()*100:.1f}%)")
    print(SEP)
    print("\n📁 Output files:")
    for f in [
        RAW_DIR / "vn100_symbols.csv",
        RAW_DIR / "ohlcv_raw.parquet",
        RAW_DIR / "market_cap_snapshot.csv",
        RAW_DIR / "book_value_annual.csv",
        CLEAN_DIR / "monthly_panel.parquet",
        CLEAN_DIR / "monthly_panel.csv",
    ]:
        size = f"{f.stat().st_size/1024:.0f} KB" if f.exists() else "not found"
        mark = "← MAIN OUTPUT" if "monthly_panel.parquet" in f.name else ""
        print(f"   {f}  [{size}] {mark}")


if __name__ == "__main__":
    main()
