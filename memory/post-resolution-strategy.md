# Post-Resolution Pair Completion Strategy

## Concept
After a 5-min BTC Up/Down period resolves, there's a window (5-300s) where:
- Winner tokens trade at $0.50-0.99 (should be $1.00)
- Loser tokens trade at $0.001-0.50 (should be $0.00)
- Buy both → redeem matched pairs at $1.00

## Economics
- Combined cost at 5-10s: $0.63-0.76 → 24-37% margin
- Combined cost at 30s: $0.83-0.90 → 10-17% margin
- Combined cost at 60s+: $0.90-0.97 → 3-10% margin

## Implementation Notes
- Uses FOK orders (taker) to sweep available liquidity
- No Binance feed needed — monitor CLOB book state only
- Must discover recently-resolved markets via CLOB scan
- Redeem via on-chain `redeem_positions()` after buying both sides
- Can also use `merge_positions()` before resolution to convert pairs → USDC

## Risk Factors
- Speed competition from queue-racers (0xd0d6, 0x63ce at 5-7s)
- Liquidity dries up fast — winner side especially
- Taker fees (up to 3.15% at 50c per Feb 2026 rule change)
- On-chain gas costs for redemption

## Key Parameters
- `max_combined_cost`: Maximum to pay for Up+Down pair (e.g., $0.95)
- `min_margin`: Minimum profit margin after fees (e.g., $0.03/pair)
- `order_size`: Shares per side per trade (e.g., 50-100)
- `scan_interval`: How often to check for resolved markets (e.g., 2s)
- `max_age_secs`: Don't buy if market resolved >300s ago (books depleted)

## Validated by CSV Data
- 15+ wallets use this exact strategy
- Profit ranges from $0.005/share (thin) to $0.21/share (queue-racers)
- 65-81% win rate across wallets
