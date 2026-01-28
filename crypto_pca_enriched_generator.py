#!/usr/bin/env python3
"""
CRYPTO PCA ENRICHED PARQUET GENERATOR FOR FOX
==============================================
Paste this entire script into Google Colab and run it.
Generates FOX-compatible crypto parquets with 5+ years history.

Colab Secrets needed:
  aws_acc (or aws_access_key_id)
  aws_sec (or aws_secret_access_key)

Output:
  s3://colab-downloads-039433203618/data/crypto_pca_enriched.parquet
  s3://colab-downloads-039433203618/data/crypto_pca_latest.parquet
"""

# !pip install -q yfinance pandas numpy scikit-learn pyarrow boto3

import os, time, warnings, random
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Dict
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
np.random.seed(42)
random.seed(42)

# === CONFIG ===
START_DATE = "2017-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
MIN_DAYS = 800
S3_BUCKET = "colab-downloads-039433203618"

CRYPTO_UNIVERSE = [
    "BTC-USD","ETH-USD","BNB-USD","SOL-USD","XRP-USD","ADA-USD","DOGE-USD",
    "DOT-USD","AVAX-USD","LINK-USD","MATIC-USD","LTC-USD","ATOM-USD","ETC-USD",
    "XLM-USD","FIL-USD","AAVE-USD","UNI-USD","MKR-USD"
]

print(f"📊 Fetching {len(CRYPTO_UNIVERSE)} cryptos from {START_DATE} to {END_DATE}...")

# === FETCH DATA ===
all_data = {}
for i, t in enumerate(CRYPTO_UNIVERSE, 1):
    print(f"[{i}/{len(CRYPTO_UNIVERSE)}] {t}...", end=" ")
    try:
        df = yf.Ticker(t).history(start=START_DATE, end=END_DATE, interval="1d")
        if len(df) >= MIN_DAYS:
            all_data[t] = df
            print(f"✓ {len(df)} rows")
        else:
            print(f"✗ only {len(df)} rows")
    except Exception as e:
        print(f"✗ {e}")
    time.sleep(0.2)

print(f"\n✅ Got {len(all_data)} coins with {MIN_DAYS}+ days history")

# === COMPUTE INDICATORS ===
print("\n🔧 Computing indicators...")

def compute_indicators(df):
    d = df.copy()
    d["Returns"] = d["Close"].pct_change()
    d["Log_Returns"] = np.log(d["Close"]/d["Close"].shift(1))
    for p in [5,10,20,50]:
        d[f"SMA_{p}"] = d["Close"].rolling(p).mean()
        d[f"EMA_{p}"] = d["Close"].ewm(span=p).mean()
    for p in [7,14,21]:
        delta = d["Close"].diff()
        gain = delta.clip(lower=0).rolling(p).mean()
        loss = (-delta.clip(upper=0)).rolling(p).mean()
        d[f"RSI_{p}"] = 100 - 100/(1 + gain/(loss+1e-10))
    d["MACD"] = d["Close"].ewm(span=12).mean() - d["Close"].ewm(span=26).mean()
    d["MACD_Signal"] = d["MACD"].ewm(span=9).mean()
    hl = d["High"] - d["Low"]
    hc = (d["High"] - d["Close"].shift(1)).abs()
    lc = (d["Low"] - d["Close"].shift(1)).abs()
    tr = pd.concat([hl,hc,lc], axis=1).max(axis=1)
    d["ATR_14"] = tr.rolling(14).mean()
    mid = d["Close"].rolling(20).mean()
    std = d["Close"].rolling(20).std()
    d["BB_Upper"] = mid + 2*std
    d["BB_Lower"] = mid - 2*std
    d["BB_Position"] = (d["Close"] - d["BB_Lower"])/(d["BB_Upper"] - d["BB_Lower"] + 1e-10)
    d["Volume_Ratio"] = d["Volume"] / d["Volume"].rolling(20).mean()
    d["HV_20"] = d["Log_Returns"].rolling(20).std() * np.sqrt(365)
    return d

crypto_ind = {t: compute_indicators(df) for t, df in all_data.items()}

# === BUILD FEATURE MATRIX ===
print("\n📊 Building feature matrix...")

frames = []
for t, df in crypto_ind.items():
    x = df.copy()
    x["Ticker"] = t
    frames.append(x)
panel = pd.concat(frames)

num_cols = panel.select_dtypes(include=[np.number]).columns.tolist()
cov = panel[num_cols].notna().mean()
keep = cov[cov >= 0.85].index.tolist()

Zblocks = []
for t in panel["Ticker"].unique():
    m = panel["Ticker"] == t
    Xi = panel.loc[m, keep].fillna(panel.loc[m, keep].median())
    Zi = (Xi - Xi.mean()) / (Xi.std() + 1e-10)
    Zi["Ticker"] = t
    Zi["Close"] = panel.loc[m, "Close"]
    Zblocks.append(Zi)

Z = pd.concat(Zblocks).dropna()
print(f"Feature matrix: {Z.shape}")

# === PCA ===
print("\n🎯 Running PCA...")
X = Z.drop(columns=["Ticker","Close"]).values
scaler = StandardScaler().fit(X)
X_scaled = scaler.transform(X)

pca = PCA(n_components=6, random_state=42).fit(X_scaled)
PCs = pca.transform(X_scaled)
print(f"Explained variance: {pca.explained_variance_ratio_.sum():.1%}")

PC_df = pd.DataFrame(PCs, index=Z.index, columns=[f"PC{i+1}" for i in range(6)])
PC_df["symbol"] = Z["Ticker"].str.replace("-USD","USD")
PC_df["close"] = Z["Close"].values
PC_df["date"] = PC_df.index

# === LEVEL/VELOCITY/ACCELERATION ===
print("\n⚡ Computing Level/Velocity/Acceleration...")

enriched = []
for sym in PC_df["symbol"].unique():
    df_s = PC_df[PC_df["symbol"]==sym].sort_values("date").copy()
    for i in range(6):
        pc = f"PC{i+1}"
        df_s[f"{pc}_Level"] = df_s[pc].ewm(span=20).mean()
        df_s[f"{pc}_Velocity"] = df_s[f"{pc}_Level"].diff()
        df_s[f"{pc}_Acceleration"] = df_s[f"{pc}_Velocity"].diff()
    enriched.append(df_s)

df_fox = pd.concat(enriched)

# Build final schema
cols = ["symbol","date","close"]
for i in range(6):
    cols += [f"PC{i+1}_Level", f"PC{i+1}_Velocity", f"PC{i+1}_Acceleration"]

df_fox = df_fox[cols].dropna()
# Handle timezone - convert if already aware, localize if naive
date_dt = pd.to_datetime(df_fox["date"])
if date_dt.dt.tz is None:
    df_fox["asof"] = date_dt.dt.tz_localize("UTC")
else:
    df_fox["asof"] = date_dt.dt.tz_convert("UTC")
df_fox = df_fox.drop(columns=["date"])
df_fox = df_fox.sort_values(["symbol","asof"]).reset_index(drop=True)

years = (df_fox["asof"].max() - df_fox["asof"].min()).total_seconds() / (365.25*24*3600)
print(f"\n✅ Final: {len(df_fox):,} rows, {df_fox.symbol.nunique()} symbols, {years:.1f} years")

# === SAVE ===
df_fox.to_parquet("crypto_pca_enriched.parquet", index=False)
df_fox[df_fox["asof"] == df_fox["asof"].max()].to_parquet("crypto_pca_latest.parquet", index=False)
print("💾 Saved parquet files")

# === UPLOAD TO S3 ===
print("\n☁️ Uploading to S3...")
try:
    import boto3
    from google.colab import userdata

    def get_secret(*names):
        for n in names:
            try:
                v = userdata.get(n)
                if v: return v
            except: pass
        return None

    key = get_secret("aws_acc","aws_access_key_id")
    secret = get_secret("aws_sec","aws_secret_access_key")

    if key and secret:
        s3 = boto3.client("s3", region_name="us-east-2",
                          aws_access_key_id=key, aws_secret_access_key=secret)
        s3.upload_file("crypto_pca_enriched.parquet", S3_BUCKET, "data/crypto_pca_enriched.parquet")
        s3.upload_file("crypto_pca_latest.parquet", S3_BUCKET, "data/crypto_pca_latest.parquet")
        print(f"✅ Uploaded to s3://{S3_BUCKET}/data/crypto_pca_enriched.parquet")
        print(f"✅ Uploaded to s3://{S3_BUCKET}/data/crypto_pca_latest.parquet")
    else:
        print("⚠️ Add Colab secrets: aws_acc + aws_sec")
except Exception as e:
    print(f"⚠️ S3 upload failed: {e}")

print("\n" + "="*60)
print("🚀 DONE! On EC2 run:")
print(f"aws s3 cp s3://{S3_BUCKET}/data/crypto_pca_enriched.parquet \\")
print("  /opt/shadow-trader/features/crypto_pca_enriched.parquet")
print("="*60)
