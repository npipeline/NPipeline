using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;
using Sample_AdvancedAggregateNode.Models;

namespace Sample_AdvancedAggregateNode.Nodes;

/// <summary>
///     AdvancedAggregateNode that calculates price volatility using complex accumulator state.
///     This node demonstrates the power of separating accumulator and result types for statistical calculations.
/// </summary>
/// <remarks>
///     This implementation uses a sophisticated accumulator that maintains:
///     - Running sum and sum of squares for variance calculation
///     - Min/max price tracking for range analysis
///     - Timestamp tracking for temporal analysis
///     The accumulator (VolatilityAccumulator) maintains intermediate computational state,
///     while the result (VolatilityResult) provides formatted analytics with derived metrics.
/// </remarks>
public class VolatilityCalculator : AdvancedAggregateNode<ValidatedTrade, string, VolatilityAccumulator, VolatilityResult>
{
    /// <summary>
    ///     Initializes a new instance of the VolatilityCalculator with tumbling windows.
    ///     Uses 5-minute tumbling windows for discrete volatility calculations.
    /// </summary>
    public VolatilityCalculator()
        : base(new AggregateNodeConfiguration<ValidatedTrade>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(5))))
    {
        Console.WriteLine("VolatilityCalculator: Initialized with 5-minute tumbling windows");
        Console.WriteLine("VolatilityCalculator: Will calculate volatility using running sum and sum of squares");
    }

    /// <summary>
    ///     Extracts the symbol key from a validated trade for grouping.
    ///     Only valid trades are included in the volatility calculation.
    /// </summary>
    /// <param name="item">The ValidatedTrade item.</param>
    /// <returns>The Symbol for grouping, or empty string for invalid trades.</returns>
    public override string GetKey(ValidatedTrade item)
    {
        // Only calculate volatility for valid trades
        if (!item.IsValid)
            return string.Empty; // Invalid trades get empty key

        var symbol = item.OriginalTrade.Symbol;
        Console.WriteLine($"VolatilityCalculator: Grouping by Symbol: {symbol} for trade {item.OriginalTrade.TradeId}");
        return symbol;
    }

    /// <summary>
    ///     Creates an initial accumulator value for a new symbol group.
    ///     This creates a default VolatilityAccumulator that will be updated as trades are accumulated.
    /// </summary>
    /// <returns>The initial accumulator with default values.</returns>
    public override VolatilityAccumulator CreateAccumulator()
    {
        return new VolatilityAccumulator(
            string.Empty, // Will be set when first trade is processed
            0,
            0m,
            0m,
            decimal.MaxValue, // Initialize min to max value
            decimal.MinValue, // Initialize max to min value
            DateTime.MinValue, // Will be set when first trade is processed
            DateTime.MinValue // Will be updated as trades are processed
        );
    }

    /// <summary>
    ///     Accumulates a validated trade into the volatility accumulator.
    ///     This method updates the running statistics needed for volatility calculation.
    /// </summary>
    /// <param name="accumulator">The current volatility accumulator.</param>
    /// <param name="item">The ValidatedTrade to accumulate.</param>
    /// <returns>The updated volatility accumulator.</returns>
    public override VolatilityAccumulator Accumulate(VolatilityAccumulator accumulator, ValidatedTrade item)
    {
        // Skip invalid trades
        if (!item.IsValid)
            return accumulator;

        var trade = item.OriginalTrade;
        var price = item.EnrichedPrice; // Use enriched price for more accurate volatility
        var newCount = accumulator.Count + 1;
        var newSum = accumulator.Sum + price;
        var newSumOfSquares = accumulator.SumOfSquares + price * price;
        var newMinPrice = Math.Min(accumulator.MinPrice, price);
        var newMaxPrice = Math.Max(accumulator.MaxPrice, price);

        // For the first trade in the group, set the symbol and initial timestamp
        if (accumulator.Count == 0)
        {
            // Calculate window boundaries based on trade timestamp
            var eventTime = trade.Timestamp;

            var windowStart = new DateTime(eventTime.Year, eventTime.Month, eventTime.Day,
                eventTime.Hour, eventTime.Minute / 5 * 5, 0, DateTimeKind.Utc); // Round down to 5-minute boundary

            var windowEnd = windowStart.AddMinutes(5);

            Console.WriteLine(
                $"VolatilityCalculator: Starting new volatility window for {trade.Symbol} " +
                $"({windowStart:HH:mm:ss} - {windowEnd:HH:mm:ss}) with initial price {price:F2}");

            return new VolatilityAccumulator(
                trade.Symbol,
                newCount,
                newSum,
                newSumOfSquares,
                price, // First trade sets both min and max
                price,
                windowStart,
                eventTime.DateTime
            );
        }

        // For subsequent trades, update the running statistics
        Console.WriteLine(
            $"VolatilityCalculator: Accumulating {trade.Symbol} price: {price:F2}, " +
            $"count: {accumulator.Count} -> {newCount}, " +
            $"min: {accumulator.MinPrice:F2} -> {newMinPrice:F2}, " +
            $"max: {accumulator.MaxPrice:F2} -> {newMaxPrice:F2}");

        return new VolatilityAccumulator(
            accumulator.Symbol,
            newCount,
            newSum,
            newSumOfSquares,
            newMinPrice,
            newMaxPrice,
            accumulator.FirstTradeTime,
            trade.Timestamp.DateTime
        );
    }

    /// <summary>
    ///     Produces the final volatility result from the accumulator.
    ///     This transforms the accumulator state into a formatted result with derived metrics.
    /// </summary>
    /// <param name="accumulator">The final volatility accumulator.</param>
    /// <returns>The volatility result with calculated metrics.</returns>
    public override VolatilityResult GetResult(VolatilityAccumulator accumulator)
    {
        if (accumulator.Count == 0)
        {
            // Return empty result for empty accumulator
            return new VolatilityResult(
                accumulator.Symbol,
                0m,
                0m,
                0m,
                0m,
                0m,
                0,
                accumulator.FirstTradeTime,
                accumulator.LastTradeTime,
                TimeSpan.FromMinutes(5)
            );
        }

        var meanPrice = accumulator.Mean;
        var volatility = accumulator.Volatility;
        var priceRange = accumulator.PriceRange;
        var windowDuration = TimeSpan.FromMinutes(5);

        Console.WriteLine(
            $"VolatilityCalculator: Final result for {accumulator.Symbol}: " +
            $"volatility={volatility:F4}, mean={meanPrice:F2}, range={priceRange:F2}, " +
            $"trades={accumulator.Count}");

        return new VolatilityResult(
            accumulator.Symbol,
            volatility,
            meanPrice,
            accumulator.MinPrice,
            accumulator.MaxPrice,
            priceRange,
            accumulator.Count,
            accumulator.FirstTradeTime,
            accumulator.LastTradeTime,
            windowDuration
        );
    }

    /// <summary>
    ///     Gets metrics about the volatility calculator's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public new (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics()
    {
        var metrics = base.GetMetrics();

        Console.WriteLine($"VolatilityCalculator Metrics: {metrics.TotalWindowsProcessed} windows processed, " +
                          $"{metrics.TotalWindowsClosed} windows closed, {metrics.MaxConcurrentWindows} max concurrent");

        return metrics;
    }

    /// <summary>
    ///     Gets the current number of active volatility windows being tracked.
    /// </summary>
    /// <returns>The current number of active windows.</returns>
    public new int GetActiveWindowCount()
    {
        var count = base.GetActiveWindowCount();
        Console.WriteLine($"VolatilityCalculator: Currently tracking {count} active volatility windows");
        return count;
    }

    /// <summary>
    ///     Calculates the efficiency of the volatility calculation.
    ///     This helps understand the computational overhead of maintaining complex accumulator state.
    /// </summary>
    /// <returns>An efficiency score based on window utilization.</returns>
    public double GetCalculationEfficiency()
    {
        var metrics = base.GetMetrics();

        if (metrics.TotalWindowsProcessed == 0)
            return 0.0;

        // Efficiency is the ratio of windows closed to windows processed
        // Higher values indicate better window utilization
        var efficiency = (double)metrics.TotalWindowsClosed / metrics.TotalWindowsProcessed;
        Console.WriteLine($"VolatilityCalculator: Calculation efficiency: {efficiency:P2}");
        return efficiency;
    }
}
