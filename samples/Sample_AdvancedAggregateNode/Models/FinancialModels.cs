using NPipeline.DataFlow;

namespace Sample_AdvancedAggregateNode.Models;

/// <summary>
///     Represents a financial trade from real-time market data
/// </summary>
public record FinancialTrade(
    string TradeId,
    string Symbol,
    string AssetClass,
    decimal Price,
    decimal Quantity,
    decimal NotionalValue,
    DateTimeOffset Timestamp,
    string TradeType,
    string TraderId,
    string PortfolioId,
    Dictionary<string, object> Properties
) : ITimestamped
{
    /// <summary>
    ///     Gets the timestamp for event-time processing
    /// </summary>
    public DateTimeOffset EventTimestamp => Timestamp;

    /// <summary>
    ///     Gets the trade direction (Buy/Sell)
    /// </summary>
    public string Direction => TradeType.Equals("BUY", StringComparison.OrdinalIgnoreCase)
        ? "BUY"
        : "SELL";

    /// <summary>
    ///     Calculates the P&L for this trade (simplified)
    /// </summary>
    public decimal CalculatePnL(decimal currentPrice)
    {
        return Direction == "BUY"
            ? (currentPrice - Price) * Quantity
            : (Price - currentPrice) * Quantity;
    }
}

/// <summary>
///     Represents a validated and enriched financial trade
/// </summary>
public record ValidatedTrade(
    FinancialTrade OriginalTrade,
    bool IsValid,
    string ValidationReason,
    decimal EnrichedPrice,
    decimal VolatilityScore,
    decimal RiskWeight
);

/// <summary>
///     Accumulator for volatility calculations using running sum and sum of squares
/// </summary>
public record VolatilityAccumulator(
    string Symbol,
    int Count,
    decimal Sum,
    decimal SumOfSquares,
    decimal MinPrice,
    decimal MaxPrice,
    DateTime FirstTradeTime,
    DateTime LastTradeTime
)
{
    /// <summary>
    ///     Calculates the mean price
    /// </summary>
    public decimal Mean => Count > 0
        ? Sum / Count
        : 0;

    /// <summary>
    ///     Calculates the variance using the sum of squares formula
    /// </summary>
    public decimal Variance => Count > 1
        ? (SumOfSquares - Sum * Sum / Count) / (Count - 1)
        : 0;

    /// <summary>
    ///     Calculates the standard deviation (volatility)
    /// </summary>
    public decimal Volatility => Variance > 0
        ? (decimal)Math.Sqrt((double)Variance)
        : 0;

    /// <summary>
    ///     Calculates the price range
    /// </summary>
    public decimal PriceRange => MaxPrice - MinPrice;
}

/// <summary>
///     Result of volatility calculation for a time window
/// </summary>
public record VolatilityResult(
    string Symbol,
    decimal Volatility,
    decimal MeanPrice,
    decimal MinPrice,
    decimal MaxPrice,
    decimal PriceRange,
    int TradeCount,
    DateTime WindowStart,
    DateTime WindowEnd,
    TimeSpan WindowDuration
)
{
    /// <summary>
    ///     Gets the annualized volatility (assuming trading days)
    /// </summary>
    public decimal AnnualizedVolatility => Volatility * (decimal)Math.Sqrt(252);

    /// <summary>
    ///     Gets the volatility as a percentage of mean price
    /// </summary>
    public double VolatilityPercentage => MeanPrice > 0
        ? (double)(Volatility / MeanPrice * 100)
        : 0;
}

/// <summary>
///     Accumulator for Value at Risk (VaR) calculations using percentile approach
/// </summary>
public record ValueAtRiskAccumulator(
    string PortfolioId,
    List<decimal> Returns,
    decimal InitialValue,
    DateTime WindowStart
)
{
    /// <summary>
    ///     Gets the count of returns
    /// </summary>
    public int Count => Returns.Count;

    /// <summary>
    ///     Adds a return to the accumulator
    /// </summary>
    public ValueAtRiskAccumulator AddReturn(decimal returnRate)
    {
        return this with
        {
            Returns = Returns.Concat(new[] { returnRate }).ToList(),
        };
    }
}

/// <summary>
///     Result of Value at Risk calculation
/// </summary>
public record ValueAtRiskResult(
    string PortfolioId,
    decimal VaR95, // 95% confidence level
    decimal VaR99, // 99% confidence level
    decimal ExpectedShortfall95,
    decimal ExpectedShortfall99,
    int SampleSize,
    decimal InitialValue,
    DateTime WindowStart,
    DateTime WindowEnd,
    TimeSpan WindowDuration
);

/// <summary>
///     Accumulator for portfolio analytics with complex state
/// </summary>
public record PortfolioAnalyticsAccumulator(
    string PortfolioId,
    Dictionary<string, decimal> AssetWeights,
    Dictionary<string, decimal> AssetReturns,
    decimal TotalValue,
    int TradeCount,
    decimal WeightedReturn,
    decimal WeightedVolatility,
    DateTime FirstTradeTime,
    DateTime LastTradeTime
)
{
    /// <summary>
    ///     Gets the number of unique assets
    /// </summary>
    public int AssetCount => AssetWeights.Count;

    /// <summary>
    ///     Calculates the portfolio return
    /// </summary>
    public decimal PortfolioReturn => TotalValue > 0
        ? WeightedReturn / TotalValue
        : 0;

    /// <summary>
    ///     Calculates the portfolio volatility
    /// </summary>
    public decimal PortfolioVolatility => TotalValue > 0
        ? WeightedVolatility / TotalValue
        : 0;
}

/// <summary>
///     Result of portfolio analytics calculation
/// </summary>
public record PortfolioAnalyticsResult(
    string PortfolioId,
    decimal TotalValue,
    decimal PortfolioReturn,
    decimal PortfolioVolatility,
    int AssetCount,
    int TradeCount,
    Dictionary<string, decimal> AssetWeights,
    Dictionary<string, decimal> AssetReturns,
    DateTime WindowStart,
    DateTime WindowEnd,
    TimeSpan WindowDuration
)
{
    /// <summary>
    ///     Calculates the Sharpe ratio (assuming risk-free rate of 2%)
    /// </summary>
    public decimal SharpeRatio => PortfolioVolatility > 0
        ? (PortfolioReturn - 0.02m) / PortfolioVolatility
        : 0;
}

/// <summary>
///     Comprehensive risk report combining all analytics
/// </summary>
public record RiskReport(
    DateTime GeneratedAt,
    List<VolatilityResult> VolatilityResults,
    List<ValueAtRiskResult> VaRResults,
    List<PortfolioAnalyticsResult> PortfolioResults,
    int TotalTradesProcessed,
    int InvalidTradesFiltered,
    TimeSpan ProcessingDuration
);

/// <summary>
///     Constants for financial asset classes and trade types
/// </summary>
public static class FinancialConstants
{
    // Asset classes
    public const string Equity = "EQUITY";
    public const string FixedIncome = "FIXED_INCOME";
    public const string Derivatives = "DERIVATIVES";
    public const string Commodities = "COMMODITIES";
    public const string Forex = "FOREX";
    public const string Crypto = "CRYPTO";

    // Trade types
    public const string Buy = "BUY";
    public const string Sell = "SELL";

    // Validation reasons
    public const string ValidTrade = "valid_trade";
    public const string InvalidPrice = "invalid_price";
    public const string InvalidQuantity = "invalid_quantity";
    public const string InvalidSymbol = "invalid_symbol";
    public const string MissingData = "missing_data";
    public const string OutOfHours = "out_of_hours";

    // Risk levels
    public const string LowRisk = "LOW";
    public const string MediumRisk = "MEDIUM";
    public const string HighRisk = "HIGH";
    public const string CriticalRisk = "CRITICAL";
}
