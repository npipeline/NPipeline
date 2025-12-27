using System.Collections.Immutable;
using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;
using Sample_AdvancedAggregateNode.Models;

namespace Sample_AdvancedAggregateNode.Nodes;

/// <summary>
///     AdvancedAggregateNode that calculates Value at Risk (VaR) using percentile-based approach.
///     This node demonstrates complex statistical calculations requiring collection and sorting of returns.
/// </summary>
/// <remarks>
///     This implementation uses a sophisticated accumulator that maintains:
///     - Collection of trade returns for percentile calculations
///     - Initial portfolio value for VaR normalization
///     - Window timing for temporal analysis
///     The accumulator (ValueAtRiskAccumulator) builds a distribution of returns,
///     while the result (ValueAtRiskResult) provides VaR metrics at different confidence levels.
///     VaR calculation methodology:
///     - 95% VaR: Maximum expected loss with 95% confidence
///     - 99% VaR: Maximum expected loss with 99% confidence
///     - Expected Shortfall: Average loss beyond VaR threshold
/// </remarks>
public class ValueAtRiskCalculator : AdvancedAggregateNode<ValidatedTrade, string, ValueAtRiskAccumulator, ValueAtRiskResult>
{
    private readonly Dictionary<string, decimal> _lastKnownPrices = new();
    private readonly Dictionary<string, decimal> _portfolioInitialValues = new();

    /// <summary>
    ///     Initializes a new instance of ValueAtRiskCalculator with sliding windows.
    ///     Uses 1-minute sliding windows every 15 seconds for real-time risk monitoring.
    /// </summary>
    public ValueAtRiskCalculator()
        : base(new AggregateNodeConfiguration<ValidatedTrade>(
            WindowAssigner.Sliding(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(15))))
    {
        Console.WriteLine("ValueAtRiskCalculator: Initialized with 1-minute sliding windows every 15 seconds");
        Console.WriteLine("ValueAtRiskCalculator: Will calculate VaR using percentile-based approach");
        InitializePortfolioValues();
    }

    /// <summary>
    ///     Extracts portfolio ID key from a validated trade for grouping.
    ///     Only valid trades are included in VaR calculation.
    /// </summary>
    /// <param name="item">The ValidatedTrade item.</param>
    /// <returns>The PortfolioId for grouping, or empty string for invalid trades.</returns>
    public override string GetKey(ValidatedTrade item)
    {
        // Only calculate VaR for valid trades
        if (!item.IsValid)
            return string.Empty; // Invalid trades get empty key

        var portfolioId = item.OriginalTrade.PortfolioId;
        Console.WriteLine($"ValueAtRiskCalculator: Grouping by PortfolioId: {portfolioId} for trade {item.OriginalTrade.TradeId}");
        return portfolioId;
    }

    /// <summary>
    ///     Creates an initial accumulator value for a new portfolio group.
    ///     This creates a default ValueAtRiskAccumulator that will collect returns.
    /// </summary>
    /// <returns>The initial accumulator with default values.</returns>
    public override ValueAtRiskAccumulator CreateAccumulator()
    {
        return new ValueAtRiskAccumulator(
            string.Empty, // Will be set when first trade is processed
            new List<decimal>(),
            0m,
            DateTime.MinValue // Will be set when first trade is processed
        );
    }

    /// <summary>
    ///     Accumulates a validated trade into the VaR accumulator.
    ///     This method calculates returns and builds the return distribution.
    /// </summary>
    /// <param name="accumulator">The current VaR accumulator.</param>
    /// <param name="item">The ValidatedTrade to accumulate.</param>
    /// <returns>The updated VaR accumulator.</returns>
    public override ValueAtRiskAccumulator Accumulate(ValueAtRiskAccumulator accumulator, ValidatedTrade item)
    {
        // Skip invalid trades
        if (!item.IsValid)
            return accumulator;

        var trade = item.OriginalTrade;
        var portfolioId = trade.PortfolioId;
        var price = item.EnrichedPrice;

        // Get or initialize portfolio data
        if (!_portfolioInitialValues.TryGetValue(portfolioId, out var existingValue))
        {
            _portfolioInitialValues[portfolioId] = trade.NotionalValue;
            Console.WriteLine($"ValueAtRiskCalculator: Initialized portfolio {portfolioId} with value {trade.NotionalValue:C}");
        }

        if (!_lastKnownPrices.TryGetValue(trade.Symbol, out var existingPrice))
            _lastKnownPrices[trade.Symbol] = price;

        // Calculate return for this trade
        var lastPrice = _lastKnownPrices[trade.Symbol];

        var returnRate = lastPrice > 0
            ? (price - lastPrice) / lastPrice
            : 0m;

        // Update last known price
        _lastKnownPrices[trade.Symbol] = price;

        // For the first trade in the group, set the portfolio ID and window timing
        if (accumulator.Count == 0)
        {
            // For sliding windows, calculate window boundaries
            var eventTime = trade.Timestamp.DateTime;

            var windowEnd = new DateTime(eventTime.Year, eventTime.Month, eventTime.Day,
                eventTime.Hour, eventTime.Minute, eventTime.Second, DateTimeKind.Utc);

            var windowStart = windowEnd.AddMinutes(-1);

            Console.WriteLine(
                $"ValueAtRiskCalculator: Starting new VaR window for {portfolioId} " +
                $"({windowStart:HH:mm:ss} - {windowEnd:HH:mm:ss}) with return {returnRate:P4}");

            var initialValue = _portfolioInitialValues.GetValueOrDefault(portfolioId, trade.NotionalValue);

            return new ValueAtRiskAccumulator(
                portfolioId,
                new List<decimal> { returnRate },
                initialValue,
                windowStart
            ).AddReturn(returnRate);
        }

        // For subsequent trades, add the return to the collection
        Console.WriteLine(
            $"ValueAtRiskCalculator: Accumulating {portfolioId} return: {returnRate:P4} for {trade.Symbol}");

        return accumulator.AddReturn(returnRate);
    }

    /// <summary>
    ///     Produces final VaR result from accumulator.
    ///     This transforms the return distribution into VaR metrics at different confidence levels.
    /// </summary>
    /// <param name="accumulator">The final VaR accumulator.</param>
    /// <returns>The VaR result with calculated risk metrics.</returns>
    public override ValueAtRiskResult GetResult(ValueAtRiskAccumulator accumulator)
    {
        if (accumulator.Count == 0)
        {
            // Return empty result for empty accumulator
            return new ValueAtRiskResult(
                accumulator.PortfolioId,
                0m,
                0m,
                0m,
                0m,
                0,
                accumulator.InitialValue,
                accumulator.WindowStart,
                accumulator.WindowStart.AddMinutes(1),
                TimeSpan.FromMinutes(1)
            );
        }

        // Sort returns for percentile calculation
        var sortedReturns = accumulator.Returns.OrderBy(x => x).ToImmutableList();
        var sampleSize = sortedReturns.Count;

        // Calculate VaR at different confidence levels
        var var95 = CalculatePercentile(sortedReturns, 0.05); // 5th percentile (95% VaR)
        var var99 = CalculatePercentile(sortedReturns, 0.01); // 1st percentile (99% VaR)

        // Calculate Expected Shortfall (Average loss beyond VaR)
        var es95 = CalculateExpectedShortfall(sortedReturns, 0.05);
        var es99 = CalculateExpectedShortfall(sortedReturns, 0.01);

        // Convert to absolute values (VaR is typically expressed as positive loss amount)
        var var95Absolute = Math.Abs(var95 * accumulator.InitialValue);
        var var99Absolute = Math.Abs(var99 * accumulator.InitialValue);
        var es95Absolute = Math.Abs(es95 * accumulator.InitialValue);
        var es99Absolute = Math.Abs(es99 * accumulator.InitialValue);

        Console.WriteLine(
            $"ValueAtRiskCalculator: Final VaR result for {accumulator.PortfolioId}: " +
            $"VaR95={var95Absolute:C}, VaR99={var99Absolute:C}, " +
            $"ES95={es95Absolute:C}, ES99={es99Absolute:C}, " +
            $"samples={sampleSize}");

        return new ValueAtRiskResult(
            accumulator.PortfolioId,
            var95Absolute,
            var99Absolute,
            es95Absolute,
            es99Absolute,
            sampleSize,
            accumulator.InitialValue,
            accumulator.WindowStart,
            accumulator.WindowStart.AddMinutes(1),
            TimeSpan.FromMinutes(1)
        );
    }

    /// <summary>
    ///     Calculates the percentile value from a sorted list of returns.
    /// </summary>
    /// <param name="sortedReturns">Sorted list of returns.</param>
    /// <param name="percentile">The percentile to calculate (0.0 to 1.0).</param>
    /// <returns>The percentile value.</returns>
    private static decimal CalculatePercentile(IImmutableList<decimal> sortedReturns, double percentile)
    {
        if (sortedReturns.Count == 0)
            return 0m;

        if (sortedReturns.Count == 1)
            return sortedReturns[0];

        var index = percentile * (sortedReturns.Count - 1);
        var lowerIndex = (int)Math.Floor(index);
        var upperIndex = (int)Math.Ceiling(index);

        if (lowerIndex == upperIndex)
            return sortedReturns[lowerIndex];

        // Linear interpolation between adjacent values
        var weight = index - lowerIndex;
        return sortedReturns[lowerIndex] * (1 - (decimal)weight) + sortedReturns[upperIndex] * (decimal)weight;
    }

    /// <summary>
    ///     Calculates Expected Shortfall (Conditional VaR) for a given confidence level.
    /// </summary>
    /// <param name="sortedReturns">Sorted list of returns.</param>
    /// <param name="alpha">The significance level (e.g., 0.05 for 95% confidence).</param>
    /// <returns>The Expected Shortfall value.</returns>
    private static decimal CalculateExpectedShortfall(IImmutableList<decimal> sortedReturns, double alpha)
    {
        if (sortedReturns.Count == 0)
            return 0m;

        var cutoffIndex = (int)Math.Ceiling(alpha * sortedReturns.Count) - 1;

        if (cutoffIndex < 0)
            return 0m;

        // Average of returns below the VaR threshold
        var tailReturns = sortedReturns.Take(cutoffIndex + 1);

        return tailReturns.Any()
            ? tailReturns.Average()
            : 0m;
    }

    /// <summary>
    ///     Initializes portfolio values with realistic starting values.
    /// </summary>
    private void InitializePortfolioValues()
    {
        // Initialize with realistic portfolio values
        _portfolioInitialValues["portfolio_equity_growth"] = 10000000m; // $10M
        _portfolioInitialValues["portfolio_fixed_income"] = 50000000m; // $50M
        _portfolioInitialValues["portfolio_mixed_balanced"] = 25000000m; // $25M
        _portfolioInitialValues["portfolio_aggressive_growth"] = 15000000m; // $15M
        _portfolioInitialValues["portfolio_conservative"] = 75000000m; // $75M
        _portfolioInitialValues["portfolio_tech_focus"] = 8000000m; // $8M
        _portfolioInitialValues["portfolio_commodities"] = 20000000m; // $20M
        _portfolioInitialValues["portfolio_forex_trading"] = 30000000m; // $30M
        _portfolioInitialValues["portfolio_crypto_assets"] = 5000000m; // $5M
        _portfolioInitialValues["portfolio_derivatives_hedging"] = 40000000m; // $40M

        Console.WriteLine($"ValueAtRiskCalculator: Initialized {_portfolioInitialValues.Count} portfolio values");
    }

    /// <summary>
    ///     Gets metrics about the VaR calculator's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public new (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics()
    {
        var metrics = base.GetMetrics();

        Console.WriteLine($"ValueAtRiskCalculator Metrics: {metrics.TotalWindowsProcessed} windows processed, " +
                          $"{metrics.TotalWindowsClosed} windows closed, {metrics.MaxConcurrentWindows} max concurrent");

        return metrics;
    }

    /// <summary>
    ///     Gets the current number of active VaR windows being tracked.
    /// </summary>
    /// <returns>The current number of active windows.</returns>
    public new int GetActiveWindowCount()
    {
        var count = base.GetActiveWindowCount();
        Console.WriteLine($"ValueAtRiskCalculator: Currently tracking {count} active VaR windows");
        return count;
    }

    /// <summary>
    ///     Calculates the statistical significance of the current VaR calculations.
    /// </summary>
    /// <returns>A confidence score based on sample sizes.</returns>
    public double GetStatisticalSignificance()
    {
        var metrics = base.GetMetrics();

        if (metrics.TotalWindowsClosed == 0)
            return 0.0;

        // Higher significance with more closed windows (indicating sufficient data)
        var significance = Math.Min(metrics.TotalWindowsClosed / 10.0, 1.0); // Cap at 1.0
        Console.WriteLine($"ValueAtRiskCalculator: Statistical significance: {significance:P2}");
        return significance;
    }
}
