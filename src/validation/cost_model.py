"""
Cost Model — Transaction cost and slippage estimation.

Provides simple cost-aware evaluation for research context.
Does NOT model realistic execution — only provides reference cost estimates.
"""


def estimate_transaction_cost(trade_value_usd, fee_rate=None):
    """
    Estimate transaction cost for a single trade.

    Args:
        trade_value_usd: Notional value in USD
        fee_rate: Fee rate (default: 0.02% maker / 0.04% taker on Binance Futures)

    Returns:
        dict with cost breakdown
    """
    if fee_rate is None:
        fee_rate = 0.0004  # 0.04% taker fee

    fee = trade_value_usd * fee_rate

    return {
        "notional_usd": trade_value_usd,
        "fee_rate": fee_rate,
        "fee_usd": round(fee, 4),
    }


def estimate_slippage(price, quantity, side, orderbook_depth=None):
    """
    Estimate slippage for a market order.

    Simple model: slippage proportional to order size.
    Without orderbook data, uses a fixed estimate.

    Args:
        price: Execution price
        quantity: Order quantity (BTC)
        side: "BUY" or "SELL"
        orderbook_depth: Optional dict with depth info

    Returns:
        dict with slippage estimate
    """
    notional = price * quantity

    # Simple fixed slippage estimate: 0.01% for small orders
    # In practice, this should use orderbook data
    slippage_pct = 0.0001  # 0.01%
    slippage_usd = notional * slippage_pct

    return {
        "price": price,
        "quantity": quantity,
        "notional_usd": notional,
        "slippage_pct": slippage_pct,
        "slippage_usd": round(slippage_usd, 4),
        "note": "Fixed estimate — use orderbook data for accurate modeling",
    }


def cost_aware_metrics(returns, trade_frequency_per_day=100, avg_notional=10000):
    """
    Compute cost-adjusted return metrics.

    Args:
        returns: List of period returns
        trade_frequency_per_day: Average trades per day
        avg_notional: Average notional per trade in USD

    Returns:
        dict with cost-adjusted metrics
    """
    if not returns:
        return {}

    total_return = sum(returns)
    mean_return = total_return / len(returns) if returns else 0

    # Daily cost estimate
    daily_cost_pct = trade_frequency_per_day * 0.0004  # 0.04% per trade
    daily_cost_usd = trade_frequency_per_day * avg_notional * 0.0004

    # Annualized (assuming 252 trading days)
    annual_cost_pct = daily_cost_pct * 252

    # Gross vs net
    import math
    gross_annual = (1 + mean_return) ** 252 - 1 if mean_return > -1 else -1
    net_annual = gross_annual - annual_cost_pct

    return {
        "mean_return_per_period": round(mean_return, 10),
        "total_return": round(total_return, 8),
        "daily_cost_pct": round(daily_cost_pct, 6),
        "daily_cost_usd": round(daily_cost_usd, 2),
        "annual_cost_pct": round(annual_cost_pct, 4),
        "gross_annual_return": round(gross_annual, 4),
        "net_annual_return": round(net_annual, 4),
        "breakeven_trades_per_day": round(mean_return / 0.0004, 1) if mean_return > 0 else "N/A",
        "note": "Estimates only — not a realistic execution model",
    }
