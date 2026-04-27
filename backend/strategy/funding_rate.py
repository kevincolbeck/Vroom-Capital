"""
Funding Rate Monitor
Fetches Bitunix's own funding rate and generates sentiment signals.
"""
import httpx
from typing import Dict, Optional, Tuple
from loguru import logger


class FundingRateMonitor:

    EXTREME_POSITIVE = 0.001   # 0.1%
    ELEVATED_POSITIVE = 0.0005 # 0.05%
    EXTREME_NEGATIVE = -0.0005 # -0.05%

    BITUNIX_URL = "https://fapi.bitunix.com/api/v1/futures/market/funding_rate"

    def __init__(self):
        self._cache: Dict[str, Optional[float]] = {}

    async def fetch_bitunix_funding(self) -> Optional[float]:
        """Fetch BTCUSDT funding rate directly from Bitunix (public endpoint)."""
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(self.BITUNIX_URL, params={"symbol": "BTCUSDT"})
                d = resp.json()
                data = d.get("data") or {}
                rate = data.get("fundingRate")
                if rate is not None:
                    return float(rate)
        except Exception as e:
            logger.debug(f"Bitunix funding fetch failed: {e}")
        return None

    async def fetch_all(self) -> Dict[str, Optional[float]]:
        """Fetch funding rate from Bitunix."""
        rate = await self.fetch_bitunix_funding()
        rates = {"bitunix": rate}
        self._cache = rates
        return rates

    def analyze_funding(self, rates: Dict[str, Optional[float]]) -> Dict:
        """
        Analyze funding rates and generate a sentiment signal.

        Returns:
            {
              "overall_sentiment": "BULLISH_CONTRARIAN" | "BEARISH_CONTRARIAN" | "NEUTRAL",
              "signal_strength": "STRONG" | "MODERATE" | "WEAK",
              "average_rate": float,
              "rates": {...},
              "description": str,
              "position_modifier": float,  # 1.0 = full size, 0.5 = half size, 0.0 = skip
            }
        """
        valid_rates = {k: v for k, v in rates.items() if v is not None}

        if not valid_rates:
            return {
                "overall_sentiment": "NEUTRAL",
                "signal_strength": "WEAK",
                "average_rate": 0.0,
                "rates": rates,
                "description": "No funding rate data available",
                "position_modifier": 1.0,
            }

        avg_rate = sum(valid_rates.values()) / len(valid_rates)
        all_positive = all(v > 0 for v in valid_rates.values())
        all_negative = all(v < 0 for v in valid_rates.values())

        # Contrarian: extreme positive funding = bearish signal (longs overcrowded)
        # Contrarian: extreme negative funding = bullish signal (shorts overcrowded)

        if avg_rate > self.EXTREME_POSITIVE:
            sentiment = "BEARISH_CONTRARIAN"
            strength = "STRONG"
            description = f"Extreme positive funding ({avg_rate*100:.3f}%) — longs overcrowded, expect dump"
            # Reduce long positions, don't fight the crowd as a long
            modifier = 0.5 if all_positive else 0.75
        elif avg_rate > self.ELEVATED_POSITIVE:
            sentiment = "BEARISH_CONTRARIAN"
            strength = "MODERATE"
            description = f"Elevated positive funding ({avg_rate*100:.3f}%) — longs crowded"
            modifier = 0.75
        elif avg_rate < self.EXTREME_NEGATIVE:
            sentiment = "BULLISH_CONTRARIAN"
            strength = "STRONG"
            description = f"Extreme negative funding ({avg_rate*100:.3f}%) — shorts overcrowded, expect pump"
            modifier = 0.5 if all_negative else 0.75
        elif avg_rate < -self.ELEVATED_POSITIVE / 2:
            sentiment = "BULLISH_CONTRARIAN"
            strength = "MODERATE"
            description = f"Negative funding ({avg_rate*100:.3f}%) — shorts crowded"
            modifier = 0.75
        else:
            sentiment = "NEUTRAL"
            strength = "WEAK"
            description = f"Neutral funding ({avg_rate*100:.3f}%) — market balanced"
            modifier = 1.0

        return {
            "overall_sentiment": sentiment,
            "signal_strength": strength,
            "average_rate": avg_rate,
            "rates": rates,
            "description": description,
            "position_modifier": modifier,
        }

    def get_trade_confirmation(self, direction: str, funding_analysis: Dict) -> Tuple[bool, str]:
        """
        Check if funding rate confirms or contradicts the trade direction.

        Returns (confirmed: bool, reason: str)
        """
        sentiment = funding_analysis.get("overall_sentiment", "NEUTRAL")
        strength = funding_analysis.get("signal_strength", "WEAK")

        # Strong contrarian signal against our direction = skip or reduce
        if direction == "LONG" and sentiment == "BEARISH_CONTRARIAN" and strength == "STRONG":
            return False, f"Funding rate strongly contrarian to LONG — skip (longs are already overcrowded)"
        if direction == "SHORT" and sentiment == "BULLISH_CONTRARIAN" and strength == "STRONG":
            return False, f"Funding rate strongly contrarian to SHORT — skip (shorts are already overcrowded)"

        # Funding confirms our direction (trade with the crowd being wrong)
        if direction == "LONG" and sentiment == "BULLISH_CONTRARIAN":
            return True, f"Funding confirms LONG — shorts are squeezable"
        if direction == "SHORT" and sentiment == "BEARISH_CONTRARIAN":
            return True, f"Funding confirms SHORT — longs are squeezable"

        return True, f"Funding rate neutral or moderate — proceed with standard sizing"
