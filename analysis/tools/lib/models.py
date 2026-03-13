"""
Domain dataclasses for wallet analysis.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Trade:
    """A single trade from the data API."""
    timestamp: datetime
    timestamp_ms: int
    side: str           # BUY or SELL
    outcome: str        # Up, Down, Yes, No
    size: float         # shares
    price: float        # 0-1
    usdc_amount: float  # size * price
    condition_id: str
    market_title: str
    asset: str = ""     # BTC, ETH, SOL, XRP
    period_elapsed_pct: Optional[float] = None
    spot_price: Optional[float] = None  # underlying price at trade time
    tx_hash: str = ""


@dataclass
class Activity:
    """A non-trade activity event (MERGE, REDEEM, SPLIT)."""
    timestamp: datetime
    activity_type: str  # MERGE, REDEEM, SPLIT
    condition_id: str
    size: float         # shares
    usdc_size: float    # USDC equivalent
    market_title: str = ""
    asset: str = ""


@dataclass
class MarketPeriod:
    """All activity for a single market period (one condition_id)."""
    condition_id: str
    market_title: str
    asset: str
    duration_minutes: int
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    trades: list[Trade] = field(default_factory=list)
    merges: list[Activity] = field(default_factory=list)
    redeems: list[Activity] = field(default_factory=list)
    splits: list[Activity] = field(default_factory=list)

    @property
    def buys(self) -> list[Trade]:
        return [t for t in self.trades if t.side == "BUY"]

    @property
    def sells(self) -> list[Trade]:
        return [t for t in self.trades if t.side == "SELL"]

    @property
    def up_buys(self) -> list[Trade]:
        return [t for t in self.buys if t.outcome in ("Up", "Yes")]

    @property
    def down_buys(self) -> list[Trade]:
        return [t for t in self.buys if t.outcome in ("Down", "No")]


@dataclass
class PeriodMetrics:
    """Computed metrics for a single market period."""
    condition_id: str
    asset: str
    duration_minutes: int
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None

    # Entry/exit timing (% of period elapsed)
    first_buy_pct: Optional[float] = None
    last_buy_pct: Optional[float] = None
    first_sell_pct: Optional[float] = None

    # Sizing
    total_buy_trades: int = 0
    total_sell_trades: int = 0
    total_up_shares: float = 0.0
    total_down_shares: float = 0.0
    avg_buy_size: float = 0.0
    max_buy_size: float = 0.0

    # Pricing
    avg_up_price: float = 0.0
    avg_down_price: float = 0.0
    combined_cost: Optional[float] = None  # avg_up + avg_down (None if not paired)

    # Pair completion
    is_paired: bool = False
    pair_ratio: float = 0.0  # min(up, down) / max(up, down) shares

    # Imbalance
    share_imbalance: float = 0.0  # |up_shares - down_shares|

    # Ladder
    up_price_levels: int = 0
    down_price_levels: int = 0

    # Sells
    num_sells: int = 0
    sell_imbalance_at_first_sell: Optional[float] = None

    # Merges / Redeems
    num_merges: int = 0
    merge_shares: float = 0.0
    num_redeems: int = 0

    # P&L — from Polymarket /positions and /closed-positions API (real data)
    real_pnl: Optional[float] = None       # cashPnl + realizedPnl from API
    estimated_pnl: Optional[float] = None  # fallback: estimated from Binance
    won: Optional[bool] = None


@dataclass
class WalletMetrics:
    """Aggregated metrics across all periods for a wallet."""
    address: str
    username: str
    analysis_days: int
    total_periods: int = 0
    total_trades: int = 0
    total_volume_usdc: float = 0.0

    # Entry timing
    avg_first_buy_pct: Optional[float] = None
    avg_last_buy_pct: Optional[float] = None
    median_first_buy_pct: Optional[float] = None

    # Sizing
    avg_buy_size: float = 0.0
    median_buy_size: float = 0.0
    max_buy_size: float = 0.0

    # Ladder
    avg_price_levels: float = 0.0
    max_price_levels: int = 0

    # Combined cost (paired periods only)
    avg_combined_cost: Optional[float] = None
    median_combined_cost: Optional[float] = None

    # Pair completion
    pair_rate: float = 0.0  # fraction of periods with both sides
    avg_pair_ratio: float = 0.0
    merge_rate: float = 0.0  # fraction of paired periods with merges

    # Imbalance
    avg_imbalance: float = 0.0
    max_imbalance: float = 0.0
    p75_imbalance: float = 0.0

    # Sells
    sell_rate: float = 0.0  # fraction of periods with sells
    avg_sell_trigger_imbalance: Optional[float] = None

    # P&L — real from API when available, estimated as fallback
    total_real_pnl: Optional[float] = None   # from /positions + /closed-positions
    total_estimated_pnl: float = 0.0          # fallback from Binance estimation
    pnl_source: str = "estimated"             # "real" or "estimated"
    win_rate: Optional[float] = None
    avg_win_pnl: float = 0.0
    avg_loss_pnl: float = 0.0
    pnl_std_dev: float = 0.0

    # Per-asset breakdown
    per_asset: dict[str, dict] = field(default_factory=dict)

    # Classification
    strategy_type: str = "unknown"  # "arb", "directional", "hybrid"

    # Raw period metrics for detail table
    period_metrics: list[PeriodMetrics] = field(default_factory=list)
