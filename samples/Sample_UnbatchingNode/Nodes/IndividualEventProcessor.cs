using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_UnbatchingNode.Nodes;

/// <summary>
///     Transform node that extracts original market data events from BatchAnalyticsWrapper.
///     This node prepares the data for unbatching by extracting the individual events from the batch analytics results.
/// </summary>
public class BatchEventExtractor : TransformNode<BatchAnalyticsWrapper, IReadOnlyCollection<MarketDataEvent>>
{
    /// <summary>
    ///     Initializes a new instance of <see cref="BatchEventExtractor" /> class.
    /// </summary>
    public BatchEventExtractor()
    {
    }

    /// <summary>
    ///     Extracts the original market data events from the batch analytics wrapper.
    ///     This prepares the data for the UnbatchingNode by providing the collection of events to be unbatched.
    /// </summary>
    /// <param name="wrapper">The batch analytics wrapper containing the events.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A collection of market data events to be unbatched.</returns>
    public override Task<IReadOnlyCollection<MarketDataEvent>> ExecuteAsync(
        BatchAnalyticsWrapper wrapper,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        if (wrapper == null || wrapper.OriginalEvents.Count == 0)
        {
            Console.WriteLine("BatchEventExtractor: Received empty wrapper, returning empty collection");
            return Task.FromResult<IReadOnlyCollection<MarketDataEvent>>(new List<MarketDataEvent>());
        }

        Console.WriteLine($"BatchEventExtractor: Extracting {wrapper.OriginalEvents.Count} events from batch {wrapper.AnalyticsResult.BatchId}");

        // Store the analytics result in context for use by downstream nodes
        context.Parameters[$"BatchAnalytics_{wrapper.AnalyticsResult.BatchId}"] = wrapper.AnalyticsResult;

        stopwatch.Stop();
        Console.WriteLine($"BatchEventExtractor: Completed extraction in {stopwatch.ElapsedMilliseconds}ms");

        return Task.FromResult<IReadOnlyCollection<MarketDataEvent>>(wrapper.OriginalEvents);
    }
}

/// <summary>
///     Transform node that converts individual market data events into alert events.
///     This node processes individual events after unbatching and generates alerts based on
///     the context provided by the batch analytics processing.
/// </summary>
public class AlertGeneratorTransform : TransformNode<MarketDataEvent, AlertEvent>
{
    private readonly decimal _priceAnomalyThreshold;
    private readonly decimal _volatilityThreshold;
    private readonly double _anomalyScoreThreshold;

    /// <summary>
    ///     Initializes a new instance of <see cref="AlertGeneratorTransform" /> class.
    /// </summary>
    /// <param name="priceAnomalyThreshold">The threshold for price anomaly detection (percentage from average).</param>
    /// <param name="volatilityThreshold">The threshold for volatility alerts (percentage).</param>
    /// <param name="anomalyScoreThreshold">The threshold for anomaly score alerts (0-1).</param>
    public AlertGeneratorTransform(
        decimal priceAnomalyThreshold = 2.0m,
        decimal volatilityThreshold = 5.0m,
        double anomalyScoreThreshold = 0.7)
    {
        _priceAnomalyThreshold = priceAnomalyThreshold;
        _volatilityThreshold = volatilityThreshold;
        _anomalyScoreThreshold = anomalyScoreThreshold;
    }

    /// <summary>
    ///     Processes individual market data events and generates alert events.
    ///     This method operates on individual events after they have been unbatched from the batch analytics results.
    /// </summary>
    /// <param name="marketEvent">The individual market data event to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>An alert event generated from the market data event.</returns>
    public override Task<AlertEvent> ExecuteAsync(
        MarketDataEvent marketEvent,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Generate alerts for this individual event
        var alerts = GenerateAlertsForEvent(marketEvent, context).ToList();

        // Return the first alert (or null if no alerts)
        return Task.FromResult(alerts.Count > 0 ? alerts[0] : null!);
    }

    private IEnumerable<AlertEvent> GenerateAlertsForEvent(MarketDataEvent marketEvent, PipelineContext context)
    {
        var alerts = new List<AlertEvent>();
        var now = DateTime.UtcNow;

        // Try to find batch analytics context from the pipeline context
        BatchAnalyticsResult? batchAnalytics = null;
        foreach (var key in context.Parameters.Keys.Where(k => k.StartsWith("BatchAnalytics_", StringComparison.OrdinalIgnoreCase)))
        {
            if (context.Parameters[key] is BatchAnalyticsResult analytics)
            {
                batchAnalytics = analytics;
                break;
            }
        }

        // If we have batch analytics, use them; otherwise simulate
        if (batchAnalytics != null && batchAnalytics.Symbol == marketEvent.Symbol)
        {
            alerts.AddRange(GenerateAlertsWithBatchContext(marketEvent, batchAnalytics, now));
        }
        else
        {
            alerts.AddRange(GenerateAlertsWithSimulatedContext(marketEvent, now));
        }

        return alerts;
    }

    private IEnumerable<AlertEvent> GenerateAlertsWithBatchContext(MarketDataEvent marketEvent, BatchAnalyticsResult batchAnalytics, DateTime now)
    {
        var alerts = new List<AlertEvent>();

        // Generate alerts based on actual batch analytics
        if (batchAnalytics.AnomalyScore >= _anomalyScoreThreshold)
        {
            var severity = batchAnalytics.AnomalyScore >= 0.9 ? "Critical" :
                          batchAnalytics.AnomalyScore >= 0.8 ? "High" : "Medium";

            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                batchAnalytics.BatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "BatchAnomaly",
                severity,
                $"Batch anomaly detected with score {batchAnalytics.AnomalyScore:F3} for {marketEvent.Symbol}",
                (decimal)batchAnalytics.AnomalyScore,
                (decimal)_anomalyScoreThreshold,
                batchAnalytics.AnomalyScore >= 0.9);
        }

        // Price anomaly alert based on deviation from batch average
        var priceDeviation = Math.Abs(marketEvent.Price - batchAnalytics.AveragePrice) / batchAnalytics.AveragePrice * 100m;
        if (priceDeviation >= _priceAnomalyThreshold)
        {
            var direction = marketEvent.Price > batchAnalytics.AveragePrice ? "Above" : "Below";
            var severity = priceDeviation >= _priceAnomalyThreshold * 2m ? "High" : "Medium";

            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                batchAnalytics.BatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "PriceAnomaly",
                severity,
                $"Individual price {direction} batch average by {priceDeviation:F2}%",
                priceDeviation,
                _priceAnomalyThreshold,
                priceDeviation >= _priceAnomalyThreshold * 2m);
        }

        // Volume spike alert
        var avgVolume = batchAnalytics.OriginalEvents.Average(e => (decimal)e.Volume);
        var volumeRatio = (decimal)marketEvent.Volume / avgVolume;

        if (volumeRatio >= 3.0m) // Volume is 3x the batch average
        {
            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                batchAnalytics.BatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "VolumeSpike",
                "Medium",
                $"Volume spike detected: {volumeRatio:F1}x batch average",
                volumeRatio,
                3.0m,
                volumeRatio >= 5.0m);
        }
    }

    private IEnumerable<AlertEvent> GenerateAlertsWithSimulatedContext(MarketDataEvent marketEvent, DateTime now)
    {
        // Simulate batch analytics for demonstration purposes
        var random = new Random(marketEvent.Symbol.GetHashCode() + marketEvent.Timestamp.GetHashCode());

        var simulatedVolatility = (decimal)(random.NextDouble() * 15.0); // 0-15% volatility
        var simulatedAnomalyScore = random.NextDouble(); // 0-1 anomaly score
        var simulatedTrend = random.NextDouble() switch
        {
            > 0.7 => "Up",
            < 0.3 => "Down",
            _ => "Stable"
        };

        var simulatedBatchId = $"Simulated-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

        // Generate alerts based on simulated batch analytics
        if (simulatedAnomalyScore >= _anomalyScoreThreshold)
        {
            var severity = simulatedAnomalyScore >= 0.9 ? "Critical" :
                          simulatedAnomalyScore >= 0.8 ? "High" : "Medium";

            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                simulatedBatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "EventAnomaly",
                severity,
                $"Event anomaly detected with score {simulatedAnomalyScore:F3} for {marketEvent.Symbol}",
                (decimal)simulatedAnomalyScore,
                (decimal)_anomalyScoreThreshold,
                simulatedAnomalyScore >= 0.9);
        }

        // Price volatility alert
        if (simulatedVolatility >= _volatilityThreshold)
        {
            var severity = simulatedVolatility >= 10.0m ? "Critical" :
                          simulatedVolatility >= 7.5m ? "High" : "Medium";

            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                simulatedBatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "PriceVolatility",
                severity,
                $"High price volatility detected: {simulatedVolatility:F2}% for {marketEvent.Symbol}",
                simulatedVolatility,
                _volatilityThreshold,
                simulatedVolatility >= 10.0m);
        }

        // Trend change alert
        if (simulatedTrend != "Stable")
        {
            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                simulatedBatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "TrendChange",
                "Medium",
                $"Price trend detected: {simulatedTrend} for {marketEvent.Symbol}",
                marketEvent.Price,
                0m,
                false);
        }

        // Volume spike alert
        if (marketEvent.Volume > 500000) // Large volume threshold
        {
            yield return new AlertEvent(
                $"Alert-{Guid.NewGuid():N}",
                now,
                simulatedBatchId,
                marketEvent.Symbol,
                marketEvent.EventId,
                "VolumeSpike",
                "Medium",
                $"High volume detected: {marketEvent.Volume:N0} for {marketEvent.Symbol}",
                marketEvent.Volume,
                500000m,
                marketEvent.Volume > 1000000);
        }
    }
}
