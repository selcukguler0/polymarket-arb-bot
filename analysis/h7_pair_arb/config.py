"""H7 pair-arb paper trader configuration."""

COIN = "btc"
DURATION_MIN = 5

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

LOG_DIR = "analysis/h7_pair_arb/logs"

# Seconds after period boundary to sample book
SAMPLE_OFFSETS_SEC = [5, 15, 30]

# Target combined ask must be below this for a profitable pair
TARGET_MAX_COMBINED_ASK = 0.96  # 4% margin minimum

# How many seconds before the next period boundary to start preparing
PREP_LEAD_SEC = 30

# API retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 1.0
REQUEST_TIMEOUT_SEC = 10
