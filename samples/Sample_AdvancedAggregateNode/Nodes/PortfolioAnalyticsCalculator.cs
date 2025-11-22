using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;
using Sample_AdvancedAggregateNode.Models;

namespace Sample_AdvancedAggregateNode.Nodes;

/// <summary>
///     AdvancedAggregateNode that calculates comprehensive portfolio analytics.
///     This node demonstrates complex accumulator patterns with weighted calculations and correlation metrics.
/// </summary>
/// <remarks>
///     This implementation uses a sophisticated accumulator that maintains:
///     - Asset weights and returns for portfolio composition analysis
///     - Weighted return and volatility calculations for portfolio metrics
///     - Trade count and timing for activity analysis
///     The accumulator (PortfolioAnalyticsAccumulator) maintains complex state for multi-dimensional analysis,
///     while the result (PortfolioAnalyticsResult) provides formatted portfolio analytics with derived metrics.
///     Portfolio analytics include:
///     - Weighted portfolio returns and volatility
///     - Asset diversification metrics
///     - Risk-adjusted performance (Sharpe ratio)
///     - Trading activity analysis
/// </remarks>
public class PortfolioAnalyticsCalculator : AdvancedAggregateNode<ValidatedTrade, string, PortfolioAnalyticsAccumulator, PortfolioAnalyticsResult>
{
    private readonly Dictionary<string, decimal> _assetVolatilities = new();

    /// <summary>
    ///     Initializes a new instance of PortfolioAnalyticsCalculator with tumbling windows.
    ///     Uses 5-minute tumbling windows for discrete portfolio analysis periods.
    /// </summary>
    public PortfolioAnalyticsCalculator()
        : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)))
    {
        Console.WriteLine("PortfolioAnalyticsCalculator: Initialized with 5-minute tumbling windows");
        Console.WriteLine("PortfolioAnalyticsCalculator: Will calculate comprehensive portfolio analytics with weighted metrics");
        InitializeAssetVolatilities();
    }

    /// <summary>
    ///     Extracts portfolio ID key from a validated trade for grouping.
    ///     Only valid trades are included in portfolio analytics.
    /// </summary>
    /// <param name="item">The ValidatedTrade item.</param>
    /// <returns>The PortfolioId for grouping, or empty string for invalid trades.</returns>
    public override string GetKey(ValidatedTrade item)
    {
        // Only calculate analytics for valid trades
        if (!item.IsValid)
            return string.Empty; // Invalid trades get empty key

        var portfolioId = item.OriginalTrade.PortfolioId;
        Console.WriteLine($"PortfolioAnalyticsCalculator: Grouping by PortfolioId: {portfolioId} for trade {item.OriginalTrade.TradeId}");
        return portfolioId;
    }

    /// <summary>
    ///     Creates an initial accumulator value for a new portfolio group.
    ///     This creates a default PortfolioAnalyticsAccumulator that will track portfolio metrics.
    /// </summary>
    /// <returns>The initial accumulator with default values.</returns>
    public override PortfolioAnalyticsAccumulator CreateAccumulator()
    {
        return new PortfolioAnalyticsAccumulator(
            string.Empty, // Will be set when first trade is processed
            new Dictionary<string, decimal>(),
            new Dictionary<string, decimal>(),
            0m,
            0,
            0m,
            0m,
            DateTime.MinValue, // Will be set when first trade is processed
            DateTime.MinValue // Will be updated as trades are processed
        );
    }

    /// <summary>
    ///     Accumulates a validated trade into the portfolio analytics accumulator.
    ///     This method updates complex portfolio state including weights, returns, and volatility.
    /// </summary>
    /// <param name="accumulator">The current portfolio analytics accumulator.</param>
    /// <param name="item">The ValidatedTrade to accumulate.</param>
    /// <returns>The updated portfolio analytics accumulator.</returns>
    public override PortfolioAnalyticsAccumulator Accumulate(PortfolioAnalyticsAccumulator accumulator, ValidatedTrade item)
    {
        // Skip invalid trades
        if (!item.IsValid)
            return accumulator;

        var trade = item.OriginalTrade;
        var symbol = trade.Symbol;
        var notionalValue = trade.NotionalValue;
        var portfolioId = trade.PortfolioId;

        // Calculate asset return (simplified - in reality would use historical data)
        var assetReturn = CalculateAssetReturn(item);
        var assetVolatility = _assetVolatilities.GetValueOrDefault(symbol, 0.2m);
        var riskWeightedValue = notionalValue * item.RiskWeight;

        // Update asset weights and returns
        var assetWeights = new Dictionary<string, decimal>(accumulator.AssetWeights);
        var assetReturns = new Dictionary<string, decimal>(accumulator.AssetReturns);

        // Update or add asset weight (weighted by risk and notional value)
        assetWeights[symbol] = assetWeights.GetValueOrDefault(symbol, 0m) + riskWeightedValue;

        // Update or add asset return (weighted by notional value)
        assetReturns[symbol] = assetReturns.GetValueOrDefault(symbol, 0m) + assetReturn * notionalValue;

        // Calculate new totals
        var newTotalValue = accumulator.TotalValue + riskWeightedValue;
        var newTradeCount = accumulator.TradeCount + 1;
        var newWeightedReturn = accumulator.WeightedReturn + assetReturn * riskWeightedValue;
        var newWeightedVolatility = accumulator.WeightedVolatility + assetVolatility * riskWeightedValue;

        // For the first trade in the group, set the portfolio ID and initial timestamp
        if (accumulator.TradeCount == 0)
        {
            // Calculate window boundaries based on trade timestamp
            var eventTime = trade.Timestamp;

            var windowStart = new DateTime(eventTime.Year, eventTime.Month, eventTime.Day,
                eventTime.Hour, eventTime.Minute / 5 * 5, 0, DateTimeKind.Utc); // Round down to 5-minute boundary

            var windowEnd = windowStart.AddMinutes(5);

            Console.WriteLine(
                $"PortfolioAnalyticsCalculator: Starting new analytics window for {portfolioId} " +
                $"({windowStart:HH:mm:ss} - {windowEnd:HH:mm:ss}) with {symbol} return {assetReturn:P4}");

            return new PortfolioAnalyticsAccumulator(
                portfolioId,
                assetWeights,
                assetReturns,
                newTotalValue,
                newTradeCount,
                newWeightedReturn,
                newWeightedVolatility,
                windowStart,
                trade.Timestamp.DateTime
            );
        }

        // For subsequent trades, update the portfolio analytics
        Console.WriteLine(
            $"PortfolioAnalyticsCalculator: Accumulating {portfolioId} {symbol}: " +
            $"return={assetReturn:P4}, weight={riskWeightedValue:C}, " +
            $"total={newTotalValue:C}, trades={newTradeCount}");

        return new PortfolioAnalyticsAccumulator(
            accumulator.PortfolioId,
            assetWeights,
            assetReturns,
            newTotalValue,
            newTradeCount,
            newWeightedReturn,
            newWeightedVolatility,
            accumulator.FirstTradeTime,
            trade.Timestamp.DateTime
        );
    }

    /// <summary>
    ///     Produces the final portfolio analytics result from the accumulator.
    ///     This transforms the accumulator state into formatted portfolio metrics.
    /// </summary>
    /// <param name="accumulator">The final portfolio analytics accumulator.</param>
    /// <returns>The portfolio analytics result with calculated metrics.</returns>
    public override PortfolioAnalyticsResult GetResult(PortfolioAnalyticsAccumulator accumulator)
    {
        if (accumulator.TradeCount == 0)
        {
            // Return empty result for empty accumulator
            return new PortfolioAnalyticsResult(
                accumulator.PortfolioId,
                0m,
                0m,
                0m,
                0,
                0,
                new Dictionary<string, decimal>(),
                new Dictionary<string, decimal>(),
                accumulator.FirstTradeTime,
                accumulator.LastTradeTime,
                TimeSpan.FromMinutes(5)
            );
        }

        // Normalize weights by total portfolio value
        var normalizedWeights = accumulator.AssetWeights.ToDictionary(
            kvp => kvp.Key,
            kvp => accumulator.TotalValue > 0
                ? kvp.Value / accumulator.TotalValue
                : 0m
        );

        // Normalize returns by total portfolio value
        var normalizedReturns = accumulator.AssetReturns.ToDictionary(
            kvp => kvp.Key,
            kvp => accumulator.TotalValue > 0
                ? kvp.Value / accumulator.TotalValue
                : 0m
        );

        // Calculate portfolio metrics
        var portfolioReturn = accumulator.PortfolioReturn;
        var portfolioVolatility = accumulator.PortfolioVolatility;
        var sharpeRatio = CalculateSharpeRatio(portfolioReturn, portfolioVolatility);

        Console.WriteLine(
            $"PortfolioAnalyticsCalculator: Final result for {accumulator.PortfolioId}: " +
            $"return={portfolioReturn:P4}, volatility={portfolioVolatility:P4}, " +
            $"sharpe={sharpeRatio:F2}, assets={accumulator.AssetCount}, trades={accumulator.TradeCount}");

        return new PortfolioAnalyticsResult(
            accumulator.PortfolioId,
            accumulator.TotalValue,
            portfolioReturn,
            portfolioVolatility,
            accumulator.AssetCount,
            accumulator.TradeCount,
            normalizedWeights,
            normalizedReturns,
            accumulator.FirstTradeTime,
            accumulator.LastTradeTime,
            TimeSpan.FromMinutes(5)
        );
    }

    /// <summary>
    ///     Calculates asset return based on trade characteristics.
    ///     In a real system, this would use historical price data.
    /// </summary>
    /// <param name="trade">The validated trade.</param>
    /// <returns>The calculated asset return.</returns>
    private static decimal CalculateAssetReturn(ValidatedTrade trade)
    {
        // Simplified return calculation based on trade direction and volatility score
        var baseReturn = trade.OriginalTrade.Direction switch
        {
            "BUY" => 0.001m, // Small positive return for buys
            "SELL" => -0.001m, // Small negative return for sells
            _ => 0m,
        };

        // Adjust by volatility score (higher volatility = higher potential returns)
        var volatilityAdjustment = trade.VolatilityScore * 0.01m;
        var randomComponent = (decimal)(new Random().NextDouble() - 0.5) * 0.002m; // ±0.1% random

        return baseReturn + volatilityAdjustment + randomComponent;
    }

    /// <summary>
    ///     Calculates Sharpe ratio for risk-adjusted performance assessment.
    /// </summary>
    /// <param name="portfolioReturn">The portfolio return.</param>
    /// <param name="portfolioVolatility">The portfolio volatility.</param>
    /// <returns>The calculated Sharpe ratio.</returns>
    private static decimal CalculateSharpeRatio(decimal portfolioReturn, decimal portfolioVolatility)
    {
        // Assume risk-free rate of 2% annually (0.00008 daily approximately)
        const decimal riskFreeRate = 0.00008m;

        return portfolioVolatility > 0
            ? (portfolioReturn - riskFreeRate) / portfolioVolatility
            : 0m;
    }

    /// <summary>
    ///     Initializes asset volatilities with realistic values.
    /// </summary>
    private void InitializeAssetVolatilities()
    {
        // Initialize with realistic annualized volatilities (converted to daily)
        _assetVolatilities["AAPL"] = 0.025m; // 25% annual -> ~1.5% daily
        _assetVolatilities["GOOGL"] = 0.030m; // 30% annual -> ~1.9% daily
        _assetVolatilities["MSFT"] = 0.022m; // 22% annual -> ~1.4% daily
        _assetVolatilities["AMZN"] = 0.035m; // 35% annual -> ~2.2% daily
        _assetVolatilities["TSLA"] = 0.050m; // 50% annual -> ~3.1% daily
        _assetVolatilities["META"] = 0.040m; // 40% annual -> ~2.5% daily
        _assetVolatilities["NVDA"] = 0.045m; // 45% annual -> ~2.8% daily
        _assetVolatilities["JPM"] = 0.028m; // 28% annual -> ~1.8% daily
        _assetVolatilities["JNJ"] = 0.018m; // 18% annual -> ~1.1% daily
        _assetVolatilities["V"] = 0.020m; // 20% annual -> ~1.3% daily

        Console.WriteLine($"PortfolioAnalyticsCalculator: Initialized {_assetVolatilities.Count} asset volatilities");
    }

    /// <summary>
    ///     Gets metrics about the portfolio analytics calculator's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public new (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics()
    {
        var metrics = base.GetMetrics();

        Console.WriteLine($"PortfolioAnalyticsCalculator Metrics: {metrics.TotalWindowsProcessed} windows processed, " +
                          $"{metrics.TotalWindowsClosed} windows closed, {metrics.MaxConcurrentWindows} max concurrent");

        return metrics;
    }

    /// <summary>
    ///     Gets the current number of active portfolio analytics windows being tracked.
    /// </summary>
    /// <returns>The current number of active windows.</returns>
    public new int GetActiveWindowCount()
    {
        var count = base.GetActiveWindowCount();
        Console.WriteLine($"PortfolioAnalyticsCalculator: Currently tracking {count} active portfolio windows");
        return count;
    }

    /// <summary>
    ///     Calculates diversification score for portfolio analysis.
    /// </summary>
    /// <param name="assetWeights">Dictionary of asset weights.</param>
    /// <returns>A diversification score (0-1, higher is more diversified).</returns>
    public static double CalculateDiversificationScore(Dictionary<string, decimal> assetWeights)
    {
        if (assetWeights.Count == 0)
            return 0.0;

        // Use Herfindahl-Hirschman Index (HHI) for diversification
        // HHI = sum of squared weights, lower = more diversified
        var hhi = assetWeights.Values.Sum(w => (double)(w * w));

        // Convert to diversification score (1 - normalized HHI)
        var maxHHI = 1.0; // Maximum HHI when all weight is in one asset
        var diversification = 1.0 - hhi / maxHHI;

        return Math.Max(0.0, diversification);
    }

    /// <summary>
    ///     Gets portfolio risk level based on volatility and return metrics.
    /// </summary>
    /// <param name="volatility">Portfolio volatility.</param>
    /// <param name="sharpeRatio">Portfolio Sharpe ratio.</param>
    /// <returns>Risk level classification.</returns>
    public static string GetRiskLevel(decimal volatility, decimal sharpeRatio)
    {
        // Risk classification based on volatility and risk-adjusted returns
        return (volatility, sharpeRatio) switch
        {
            (< 0.01m, > 1.0m) => FinancialConstants.LowRisk,
            (< 0.02m, > 0.5m) => FinancialConstants.MediumRisk,
            (< 0.03m, > 0.0m) => FinancialConstants.HighRisk,
            _ => FinancialConstants.CriticalRisk,
        };
    }
}
