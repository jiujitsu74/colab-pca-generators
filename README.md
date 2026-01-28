# Colab PCA Enriched Parquet Generators

Generate FOX-compatible PCA enriched parquet files for backtesting.

## Quick Start (Colab)

```python
!git clone https://github.com/jiujitsu74/colab-pca-generators.git
%cd colab-pca-generators
!pip install -q yfinance pandas numpy scikit-learn pyarrow boto3
%run crypto_pca_enriched_generator.py
```

Or just copy/paste the script contents into a Colab cell.

## Colab Secrets Required

Add these to Colab Secrets (🔑 icon in left sidebar):
- `aws_acc` - Your AWS Access Key ID
- `aws_sec` - Your AWS Secret Access Key

## Scripts

| Script | Asset | Output |
|--------|-------|--------|
| `crypto_pca_enriched_generator.py` | Crypto | `s3://colab-downloads-039433203618/data/crypto_pca_enriched.parquet` |
| `equities_pca_enriched_generator.py` | Equities | (TODO) |

## After Running

On EC2, pull the new parquets:

```bash
# Crypto
aws s3 cp s3://colab-downloads-039433203618/data/crypto_pca_enriched.parquet \
  /opt/shadow-trader/features/crypto_pca_enriched.parquet

# Verify
/opt/shadow-trader/bin/diag_find_asset_parquets.py
```

## FOX Schema

The generated parquets follow the FOX-compatible schema:

```
symbol           string   (e.g., "BTCUSD")
asof             datetime (UTC)
close            float64
PC1_Level        float64
PC1_Velocity     float64
PC1_Acceleration float64
PC2_Level        float64
...
PC6_Acceleration float64
```

## Requirements

- 5+ years of historical data (FOX minimum)
- Level/Velocity/Acceleration computed per symbol
- UTC timestamps
