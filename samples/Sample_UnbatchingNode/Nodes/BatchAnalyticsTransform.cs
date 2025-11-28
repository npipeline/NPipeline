using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_UnbatchingNode.Nodes;

/// <summary>
///     Wrapper that contains both the batch analytics result and the original events.
///     This allows the UnbatchingNode to extract and unbatch the original events.
/// </summary>
public record BatchAnalyticsWrapper(
    BatchAnalyticsResult AnalyticsResult,
    IReadOnlyList<MarketDataEvent> OriginalEvents);

/// <summary>
///     Transform node that processes batches of market data events and generates analytics results.
///     This node demonstrates efficient batch processing for financial analytics calculations.
/// </summary>
public class BatchAnalyticsTransform : TransformNode<IReadOnlyCollection<MarketDataEvent>, BatchAnalyticsWrapper>
{
    private readonly double _anomalyThreshold;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BatchAnalyticsTransform" /> class.
    /// </summary>
    /// <param name="anomalyThreshold">The threshold for anomaly detection (0-1, higher means more sensitive).</param>
    public BatchAnalyticsTransform(double anomalyThreshold = 0.7)
    {
        _anomalyThreshold = anomalyThreshold;
    }

    /// <summary>
    ///     Processes a batch of market data events and generates comprehensive analytics results.
    /// </summary>
    /// <param name="batch">The batch of market data events to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A batch analytics wrapper containing the results and original events.</returns>
    public override Task<BatchAnalyticsWrapper> ExecuteAsync(
        IReadOnlyCollection<MarketDataEvent> batch,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        if (batch == null || batch.Count == 0)
        {
            Console.WriteLine("Received empty batch, returning null result");
            return Task.FromResult<BatchAnalyticsWrapper>(null!);
        }

        Console.WriteLine($"Processing batch of {batch.Count} market data events for symbol {batch.First().Symbol}");

        // Calculate analytics metrics
        var symbol = batch.First().Symbol;
        var prices = batch.Select(e => e.Price).ToList();
        var volumes = batch.Select(e => e.Volume).ToList();

        // Basic statistics
        var averagePrice = prices.Average();
        var minPrice = prices.Min();
        var maxPrice = prices.Max();
        var priceVolatility = (maxPrice - minPrice) / averagePrice * 100m;

        // Volume Weighted Average Price (VWAP)
        var totalValue = batch.Sum(e => e.Price * e.Volume);
        var totalVolume = batch.Sum(e => e.Volume);

        var volumeWeightedAveragePrice = totalVolume > 0
            ? totalValue / totalVolume
            : 0m;

        // Price trend analysis
        var priceTrend = CalculatePriceTrend(prices);

        // Anomaly detection based on price volatility and volume spikes
        var avgVolume = volumes.Average();
        var maxVolume = volumes.Max();

        var volumeSpikeRatio = avgVolume > 0
            ? maxVolume / avgVolume
            : 0;

        var anomalyScore = Math.Min(1.0, (double)(priceVolatility / 10m) + volumeSpikeRatio / 10.0);

        var batchId = $"Batch-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

        var analyticsResult = new BatchAnalyticsResult(
            batchId,
            DateTime.UtcNow,
            symbol,
            batch.Count,
            Math.Round(averagePrice, 4),
            Math.Round(priceVolatility, 4),
            Math.Round(volumeWeightedAveragePrice, 4),
            priceTrend,
            Math.Round(anomalyScore, 4),
            stopwatch.ElapsedMilliseconds,
            batch.ToList());

        // Create wrapper that contains both analytics result and original events
        var wrapper = new BatchAnalyticsWrapper(analyticsResult, batch.ToList());

        stopwatch.Stop();

        Console.WriteLine($"Batch analytics completed in {stopwatch.ElapsedMilliseconds}ms - " +
                          $"Avg Price: {analyticsResult.AveragePrice}, Volatility: {analyticsResult.PriceVolatility}%, " +
                          $"Trend: {analyticsResult.PriceTrend}, Anomaly Score: {analyticsResult.AnomalyScore}");

        return Task.FromResult(wrapper);
    }

    private static string CalculatePriceTrend(IReadOnlyList<decimal> prices)
    {
        if (prices.Count < 2)
            return "Stable";

        // Simple linear regression to determine trend
        var n = prices.Count;
        var sumX = 0.0;
        var sumY = 0.0;
        var sumXY = 0.0;
        var sumX2 = 0.0;

        for (var i = 0; i < n; i++)
        {
            var x = i;
            var y = (double)prices[i];
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }

        var slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        var avgPrice = sumY / n;
        var percentChange = Math.Abs(slope / avgPrice * 100);

        return slope switch
        {
            > 0 when percentChange > 1.0 => "Up",
            < 0 when percentChange > 1.0 => "Down",
            _ => "Stable",
        };
    }
}
