using System;
using System.Collections.Generic;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;
using Sample_UnbatchingNode.Nodes;

namespace Sample_UnbatchingNode;

/// <summary>
///     Unbatching pipeline demonstrating the conversion of batched analytics results back to individual item streams.
///     This pipeline implements a complete financial trading system flow with unbatching:
///     1. MarketDataSource generates individual market data events
///     2. BatchingNode collects events into batches for analytics processing
///     3. BatchAnalyticsTransform processes batches efficiently with analytics calculations
///     4. BatchEventExtractor extracts original events from batch analytics results
///     5. UnbatchingNode converts batched events back to individual market data events (UNBATCHING)
///     6. AlertGeneratorTransform converts individual events to alerts based on batch analytics insights
///     7. RealTimeAlertingSink processes individual alert events for real-time monitoring
/// </summary>
/// <remarks>
///     This implementation demonstrates the key unbatching pattern:
///     - Individual items are batched for efficient processing
///     - Batch analytics provide insights that apply to all items in the batch
///     - Unbatching converts batch results back to individual events for downstream processing
///     - Real-time alerting requires individual events, not batched results
///     The pipeline shows how unbatching enables:
///     - Efficient batch processing for analytics
///     - Individual event processing for real-time operations
///     - Conversion between batched and individual data flows as needed
/// </remarks>
public class UnbatchingStreamConversionPipeline : IPipelineDefinition
{
    private double _anomalyScoreThreshold;
    private int _batchSize;
    private TimeSpan _batchTimeout;
    private int _marketDataEventCount;
    private TimeSpan _marketDataInterval;
    private decimal _priceAnomalyThreshold;
    private decimal _volatilityThreshold;

    /// <summary>
    ///     Initializes a new instance of the <see cref="UnbatchingStreamConversionPipeline" /> class.
    /// </summary>
    /// <param name="batchSize">The maximum number of items in a batch.</param>
    /// <param name="batchTimeout">The maximum time to wait before emitting a batch.</param>
    /// <param name="marketDataEventCount">The number of market data events to generate.</param>
    /// <param name="marketDataInterval">The interval between market data events.</param>
    /// <param name="priceAnomalyThreshold">The threshold for price anomaly detection.</param>
    /// <param name="volatilityThreshold">The threshold for volatility alerts.</param>
    /// <param name="anomalyScoreThreshold">The threshold for anomaly score alerts.</param>
    public UnbatchingStreamConversionPipeline(
        int batchSize = 15,
        TimeSpan? batchTimeout = null,
        int marketDataEventCount = 100,
        TimeSpan? marketDataInterval = null,
        decimal priceAnomalyThreshold = 2.0m,
        decimal volatilityThreshold = 5.0m,
        double anomalyScoreThreshold = 0.7)
    {
        _batchSize = batchSize;
        _batchTimeout = batchTimeout ?? TimeSpan.FromSeconds(2);
        _marketDataEventCount = marketDataEventCount;
        _marketDataInterval = marketDataInterval ?? TimeSpan.FromMilliseconds(50);
        _priceAnomalyThreshold = priceAnomalyThreshold;
        _volatilityThreshold = volatilityThreshold;
        _anomalyScoreThreshold = anomalyScoreThreshold;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="UnbatchingStreamConversionPipeline" /> class.
    /// </summary>
    public UnbatchingStreamConversionPipeline()
    {
        _batchSize = 15;
        _batchTimeout = TimeSpan.FromSeconds(2);
        _marketDataEventCount = 100;
        _marketDataInterval = TimeSpan.FromMilliseconds(50);
        _priceAnomalyThreshold = 2.0m;
        _volatilityThreshold = 5.0m;
        _anomalyScoreThreshold = 0.7;
    }

    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a pipeline flow that demonstrates both batching and unbatching:
    ///     MarketDataSource -> BatchingNode -> BatchAnalyticsTransform -> BatchEventExtractor -> UnbatchingNode -> AlertGeneratorTransform -> RealTimeAlertingSink
    ///     The pipeline processes data through these stages:
    ///     1. Source generates individual market data events from multiple exchanges
    ///     2. BatchingNode collects individual events into batches for efficient analytics processing
    ///     3. Transform processes batches with comprehensive analytics calculations
    ///     4. BatchEventExtractor extracts original events from batch analytics results
    ///     5. UnbatchingNode converts batched events back to individual market data events (UNBATCHING)
    ///     6. AlertGeneratorTransform converts individual events to alerts based on batch analytics insights
    ///     7. Sink processes individual alert events for real-time monitoring and alerting
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read parameters from context if available
        if (context.Parameters.TryGetValue("BatchSize", out var batchSizeObj) && batchSizeObj is int batchSize)
            _batchSize = batchSize;

        if (context.Parameters.TryGetValue("BatchTimeout", out var batchTimeoutObj) && batchTimeoutObj is TimeSpan batchTimeout)
            _batchTimeout = batchTimeout;

        if (context.Parameters.TryGetValue("MarketDataEventCount", out var eventCountObj) && eventCountObj is int eventCount)
            _marketDataEventCount = eventCount;

        if (context.Parameters.TryGetValue("MarketDataInterval", out var intervalObj) && intervalObj is TimeSpan interval)
            _marketDataInterval = interval;

        if (context.Parameters.TryGetValue("PriceAnomalyThreshold", out var priceThresholdObj) && priceThresholdObj is decimal priceThreshold)
            _priceAnomalyThreshold = priceThreshold;

        if (context.Parameters.TryGetValue("VolatilityThreshold", out var volatilityThresholdObj) && volatilityThresholdObj is decimal volatilityThreshold)
            _volatilityThreshold = volatilityThreshold;

        if (context.Parameters.TryGetValue("AnomalyScoreThreshold", out var anomalyThresholdObj) && anomalyThresholdObj is double anomalyThreshold)
            _anomalyScoreThreshold = anomalyThreshold;

        // Add the source node that generates individual market data events
        var marketDataSource = builder.AddSource<MarketDataSource, MarketDataEvent>("market-data-source");

        // Add the batching node that collects individual events into batches for analytics
        var batching = builder.AddBatcher<MarketDataEvent>("batching-node", _batchSize, _batchTimeout);

        // Add the batch analytics transform that processes batches efficiently
        var batchAnalytics = builder.AddTransform<BatchAnalyticsTransform, IReadOnlyCollection<MarketDataEvent>, BatchAnalyticsWrapper>(
            "batch-analytics-transform");

        // Add the batch event extractor that extracts original events from batch analytics results
        var batchEventExtractor = builder.AddTransform<BatchEventExtractor, BatchAnalyticsWrapper, IReadOnlyCollection<MarketDataEvent>>(
            "batch-event-extractor");

        // Add the unbatching node that converts batched events back to individual market data events (UNBATCHING)
        // This is the key component that demonstrates unbatching functionality
        var unbatching = builder.AddUnbatcher<MarketDataEvent>("unbatching-node");

        // Configure the UnbatchingExecutionStrategy for the UnbatchingNode
        builder.WithExecutionStrategy(unbatching, new UnbatchingExecutionStrategy());

        // Add the alert generator transform that converts individual events to alerts based on batch analytics
        var alertGenerator = builder.AddTransform<AlertGeneratorTransform, MarketDataEvent, AlertEvent>(
            "alert-generator-transform");

        // Add the real-time alerting sink that processes individual alert events
        var alertingSink = builder.AddSink<RealTimeAlertingSink, AlertEvent>("real-time-alerting-sink");

        // Connect the nodes in a linear flow: source -> batching -> batchAnalytics -> batchEventExtractor -> unbatching -> alertGenerator -> alertingSink
        builder.Connect(marketDataSource, batching);
        builder.Connect(batching, batchAnalytics);
        builder.Connect(batchAnalytics, batchEventExtractor);
        builder.Connect(batchEventExtractor, unbatching);
        builder.Connect(unbatching, alertGenerator);
        builder.Connect(alertGenerator, alertingSink);

        // Store pipeline configuration in context for access by nodes
        context.Parameters["BatchSize"] = _batchSize;
        context.Parameters["BatchTimeout"] = _batchTimeout;
        context.Parameters["MarketDataEventCount"] = _marketDataEventCount;
        context.Parameters["MarketDataInterval"] = _marketDataInterval;
        context.Parameters["PriceAnomalyThreshold"] = _priceAnomalyThreshold;
        context.Parameters["VolatilityThreshold"] = _volatilityThreshold;
        context.Parameters["AnomalyScoreThreshold"] = _anomalyScoreThreshold;
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"UnbatchingNode Sample:

This sample demonstrates the conversion of batched analytics results back to individual item streams:
- Individual market data events are batched for efficient analytics processing
- Batch analytics provide comprehensive insights across multiple events
- Unbatching converts batch results back to individual market data events for real-time processing
- Real-time alerting requires individual events, not batched results

The pipeline flow:
1. MarketDataSource generates individual market data events from multiple exchanges
2. BatchingNode collects individual events into batches based on size and time thresholds
3. BatchAnalyticsTransform processes batches efficiently with comprehensive analytics calculations
4. BatchEventExtractor extracts original events from batch analytics results
5. UnbatchingNode converts batched events back to individual market data events (UNBATCHING)
6. AlertGeneratorTransform converts individual events to alerts based on batch analytics insights
7. RealTimeAlertingSink processes individual alert events for real-time monitoring and alerting

Key concepts demonstrated:
- Unbatching pattern: Converting batch results back to individual item streams
- Batch analytics for efficiency: Processing multiple items together for better performance
- Individual event processing for real-time operations: Converting back to individual events when needed
- Financial trading scenario: Market data analytics and real-time alerting
- Threshold-based alerting: Generating alerts based on analytics insights

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions with configurable parameters
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic

The unbatching pattern is essential when:
- You need efficient batch processing for analytics
- But downstream systems require individual events
- Real-time processing is needed after batch analytics
- You want to combine the benefits of both batched and individual processing";
    }
}
