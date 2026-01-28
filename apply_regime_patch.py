#!/usr/bin/env python3
"""Apply regime contract mapping patch to regime_aware_backtest.py"""

FILE_PATH = "/opt/shadow-trader/fox/regime_aware_backtest.py"

MAPPING_CODE = '''

# Regime contract mapping - BRAIN labels to FOX classifier labels
BRAIN_TO_FOX_REGIME = {
    "MOMENTUM_STRONG": "TREND",
    "MEANREV_CHOPPY": "CONSOLIDATION",
    "RISK_OFF_BEARISH": "CRASH",
    "CRISIS": "CRASH",
    "USD_STRENGTH": "TREND",
    "FX_CONSOLIDATION": "CONSOLIDATION",
    "FX_CRISIS": "HIGH_VOL",
    "CONSOLIDATION_LOW_VOL": "LOW_VOL",
    "HIGH_VOL_CRISIS": "HIGH_VOL",
    "BULL_TREND": "TREND",
    "COMMODITY_BULL": "TREND",
    "COMMODITY_BEAR": "CRASH",
    "COMMODITY_RANGE": "CONSOLIDATION",
    "NEUTRAL": "CONSOLIDATION",
    "UNKNOWN": "CONSOLIDATION",
}

def brain_to_fox(brain_label):
    """Map BRAIN spawn regime label to FOX classifier label."""
    return BRAIN_TO_FOX_REGIME.get(brain_label, brain_label)
'''

OLD_LINE = "    in_regime_mask = daily['regime'].loc[strategy_returns.index] == target_regime"

NEW_LINES = '''    # Map BRAIN regime to FOX classifier label
    fox_target = brain_to_fox(target_regime)
    logger.info(f"  [REGIME] Mapping BRAIN:{target_regime} -> FOX:{fox_target}")
    in_regime_mask = daily['regime'].loc[strategy_returns.index] == fox_target'''


def main():
    # Read file
    with open(FILE_PATH) as f:
        content = f.read()

    # Check if already patched
    if "BRAIN_TO_FOX_REGIME" in content:
        print("Already patched")
        return

    # Find insertion point (after logger line)
    idx = content.find("logger = logging.getLogger")
    if idx < 0:
        print("ERROR: Could not find logger line")
        return

    end_of_line = content.find("\n", idx)
    content = content[:end_of_line + 1] + MAPPING_CODE + content[end_of_line + 1:]

    # Replace comparison line
    if OLD_LINE in content:
        content = content.replace(OLD_LINE, NEW_LINES)
        print("Replaced in_regime_mask line")
    else:
        print("WARNING: Could not find in_regime_mask line to replace")

    # Write back
    with open(FILE_PATH, "w") as f:
        f.write(content)

    print("Patch applied successfully")


if __name__ == "__main__":
    main()
