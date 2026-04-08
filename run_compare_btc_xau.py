"""Compare BTCUSD vs XAUUSD M1 data — scaling analysis for DT818_exp."""
import pandas as pd
import numpy as np
from pathlib import Path

XAU_PATH = Path("output/market_data/xauusd_m1_6m.parquet")
BTC_PATH = Path("output/market_data/btcusd_m1_6m.parquet")

POINT = 0.01  # same for both symbols at this broker

# Current best ET#1 XAUUSD params (in points)
XAU_PARAMS = {
    "S1 M30": dict(tp=14500, sl=10500, spread=45),
    "S2 H4":  dict(tp=20000, sl=8500,  spread=45),
    "S3 H1":  dict(tp=22500, sl=7000,  spread=45),
}
BTC_SPREAD = 1706


def resample_ohlc(m1, freq):
    m1 = m1.set_index("datetime")
    df = m1.resample(freq, label="left", closed="left").agg(
        open=("open","first"), high=("high","max"),
        low=("low","min"), close=("close","last"),
    ).dropna().reset_index()
    return df


def print_section(title):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print('='*65)


def analyse(df, label, spread_pts, timeframes):
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
    avg_price = df["close"].mean()
    print(f"\n  {label}")
    print(f"  Avg price: ${avg_price:,.0f}  |  Data: {df['datetime'].iloc[0].date()} to {df['datetime'].iloc[-1].date()}  ({len(df):,} M1 bars)")
    print(f"  Spread: {spread_pts} pts = ${spread_pts * POINT:.2f} = {spread_pts * POINT / avg_price * 100:.3f}%")

    # Daily range
    daily = df.resample("1D", on="datetime").agg(
        high=("high","max"), low=("low","min"), close=("close","last")
    ).dropna()
    daily["range"] = daily["high"] - daily["low"]
    daily["range_pct"] = daily["range"] / daily["close"] * 100
    print(f"  Daily range: mean=${daily['range'].mean():,.0f} ({daily['range_pct'].mean():.2f}%)  "
          f"median=${daily['range'].median():,.0f}  p90=${daily['range'].quantile(0.9):,.0f}")

    # M1 bar range
    df["m1_range"] = df["high"] - df["low"]
    print(f"  M1 bar range: mean=${df['m1_range'].mean():.2f}  median=${df['m1_range'].median():.2f}")

    # Session M1 ranges
    df["hour"] = df["datetime"].dt.hour
    hourly = df.groupby("hour")["m1_range"].mean()
    for sess, h in [("Asia(0-6)",hourly.loc[0:6].mean()),("London(7-11)",hourly.loc[7:11].mean()),
                    ("NY(12-16)",hourly.loc[12:16].mean()),("After-NY(17-23)",hourly.loc[17:23].mean())]:
        print(f"    {sess:<18} M1 mean range: ${h:.2f}")

    # TF ranges
    print(f"  Timeframe ranges:")
    for tf_label, freq in timeframes:
        tf = resample_ohlc(df[["datetime","open","high","low","close"]], freq)
        tf["range"] = tf["high"] - tf["low"]
        tf["atr"]   = tf["range"].rolling(14).mean()
        atr_mean    = tf["atr"].mean()
        atr_pts     = atr_mean / POINT
        print(f"    {tf_label:<8} bar range mean=${tf['range'].mean():,.2f}  ATR(14)=${atr_mean:,.2f} = {atr_pts:,.0f} pts")

    return avg_price, daily["range"].mean(), df["m1_range"].mean()


def main():
    print("Loading data...")
    xau = pd.read_parquet(XAU_PATH)
    btc = pd.read_parquet(BTC_PATH)
    xau["datetime"] = pd.to_datetime(xau["datetime"]).dt.tz_localize(None)
    btc["datetime"] = pd.to_datetime(btc["datetime"]).dt.tz_localize(None)

    # Align to overlapping window for fair comparison
    overlap_start = max(xau["datetime"].min(), btc["datetime"].min())
    overlap_end   = min(xau["datetime"].max(), btc["datetime"].max())
    xau_ov = xau[(xau["datetime"] >= overlap_start) & (xau["datetime"] <= overlap_end)]
    btc_ov = btc[(btc["datetime"] >= overlap_start) & (btc["datetime"] <= overlap_end)]
    print(f"  Overlap window: {overlap_start.date()} to {overlap_end.date()}")

    TFS = [("M30","30min"), ("H1","1h"), ("H4","4h")]

    print_section("INDIVIDUAL ANALYSIS")
    xau_price, xau_daily, xau_m1 = analyse(xau_ov, "XAUUSD", 45,  TFS)
    btc_price, btc_daily, btc_m1 = analyse(btc_ov, "BTCUSD", 1706, TFS)

    print_section("SCALING ANALYSIS — XAU params to BTC")
    print("""
  DT818_exp uses fixed TP/SL in points. Point=0.01 for both symbols.
  To preserve the same % move target, scale by price ratio:
    BTC_pts = XAU_pts * (BTC_price / XAU_price)
  To preserve the same R:R ratio, scale TP and SL by the same factor.
""")

    price_ratio = btc_price / xau_price
    print(f"  Price ratio: BTC/XAU = {btc_price:,.0f} / {xau_price:,.0f} = {price_ratio:.2f}x")
    print(f"  Daily range ratio: BTC/XAU = {btc_daily:,.0f} / {xau_daily:.0f} = {btc_daily/xau_daily:.2f}x")
    print(f"  M1 bar range ratio: BTC/XAU = {btc_m1:.2f} / {xau_m1:.2f} = {btc_m1/xau_m1:.2f}x\n")

    print(f"  {'Stream':<8}  {'XAU TP/SL (pts)':<20}  {'XAU TP/SL ($)':<20}  "
          f"{'BTC scaled TP/SL (pts)':<25}  {'BTC TP/SL ($)'}")
    print(f"  {'-'*100}")

    for stream, p in XAU_PARAMS.items():
        tp_usd = p["tp"] * POINT
        sl_usd = p["sl"] * POINT
        tp_pct = tp_usd / xau_price * 100
        sl_pct = sl_usd / xau_price * 100

        # Scale by price ratio
        btc_tp_pts = int(round(p["tp"] * price_ratio / 500) * 500)
        btc_sl_pts = int(round(p["sl"] * price_ratio / 500) * 500)
        btc_tp_usd = btc_tp_pts * POINT
        btc_sl_usd = btc_sl_pts * POINT

        print(f"  {stream:<8}  TP={p['tp']:>6} SL={p['sl']:>6}     "
              f"TP=${tp_usd:>6.0f}({tp_pct:.1f}%) SL=${sl_usd:>5.0f}({sl_pct:.1f}%)  "
              f"TP={btc_tp_pts:>7} SL={btc_sl_pts:>7}           "
              f"TP=${btc_tp_usd:>7.0f} SL=${btc_sl_usd:>6.0f}")

    print_section("SPREAD IMPACT COMPARISON")
    print(f"""
  Spread as % of TP:
  {'Stream':<8}  {'XAU spread/TP':<20}  {'BTC spread/TP (scaled)'}""")
    for stream, p in XAU_PARAMS.items():
        xau_spread_pct = 45 / p["tp"] * 100
        btc_tp_pts = int(round(p["tp"] * price_ratio / 500) * 500)
        btc_spread_pct = BTC_SPREAD / btc_tp_pts * 100
        print(f"  {stream:<8}  XAU: {45}/{p['tp']} = {xau_spread_pct:.2f}%         "
              f"BTC: {BTC_SPREAD}/{btc_tp_pts} = {btc_spread_pct:.2f}%")

    print_section("FEASIBILITY ASSESSMENT")
    btc_daily_pct = btc_daily / btc_price * 100
    xau_daily_pct = xau_daily / xau_price * 100

    # For S1 M30, what % of daily range does TP represent?
    xau_tp_pct = XAU_PARAMS["S1 M30"]["tp"] * POINT / xau_price * 100
    btc_tp_equiv_pct = xau_tp_pct
    btc_tp_equiv_usd = btc_tp_equiv_pct / 100 * btc_price
    btc_tp_equiv_pts = int(btc_tp_equiv_usd / POINT)

    print(f"""
  Daily range:
    XAUUSD: ${xau_daily:,.0f} ({xau_daily_pct:.2f}% of price)
    BTCUSD: ${btc_daily:,.0f} ({btc_daily_pct:.2f}% of price)

  XAUUSD TP as % of daily range (S1): {XAU_PARAMS["S1 M30"]["tp"]*POINT / xau_daily * 100:.1f}%
  BTCUSD equiv TP as % of daily range: {btc_tp_equiv_usd / btc_daily * 100:.1f}%

  Spread cost:
    XAUUSD: {45*POINT:.2f} USD per trade  ({45/XAU_PARAMS["S1 M30"]["sl"]*100:.2f}% of SL)
    BTCUSD: {BTC_SPREAD*POINT:.2f} USD per trade  ({BTC_SPREAD/int(round(XAU_PARAMS["S1 M30"]["sl"]*price_ratio/500)*500)*100:.2f}% of scaled SL)

  Key concerns for BTCUSD:
    1. Spread is {BTC_SPREAD//45:.0f}x higher — significant drag on a trend-following system
    2. Only ~10 weeks of broker data available — insufficient for 8-week IS reopt
    3. BTC trades 24/7 including weekends — more bars, different session dynamics
    4. Higher volatility ({btc_daily_pct:.1f}% vs {xau_daily_pct:.1f}% daily) — TP/SL need larger values
    5. Same point size (0.01) — scaled TP/SL in points would be ~{price_ratio:.0f}x larger
""")


if __name__ == "__main__":
    main()
