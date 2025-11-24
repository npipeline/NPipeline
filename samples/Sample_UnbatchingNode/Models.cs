using System;
using System.Collections.Generic;

namespace Sample_UnbatchingNode;

/// <summary>
///     Represents an individual market data event from a financial trading system.
///     This model demonstrates individual items that are batched for analytics processing.
/// </summary>
/// <param name="EventId">The unique identifier for this market data event.</param>
/// <param name="Timestamp">When the market data event occurred.</param>
/// <param name="Symbol">The trading symbol (e.g., "AAPL", "MSFT").</param>
/// <param name="Price">The current price of the security.</param>
/// <param name="Volume">The trading volume.</param>
/// <param name="Exchange">The exchange where the trade occurred.</param>
/// <param name="EventType">The type of market data event (Trade, Quote, etc.).</param>
public record MarketDataEvent(
    string EventId,
    DateTime Timestamp,
    string Symbol,
    decimal Price,
    long Volume,
    string Exchange,
    string EventType);

/// <summary>
///     Represents the result of batch analytics processing on market data events.
///     This model shows aggregated analytics results that need to be converted back to individual events.
/// </summary>
/// <param name="BatchId">Unique identifier for this analytics batch.</param>
/// <param name="BatchTimestamp">When this batch was processed.</param>
/// <param name="Symbol">The trading symbol for these analytics.</param>
/// <param name="EventCount">Number of market data events in this batch.</param>
/// <param name="AveragePrice">Average price across all events in the batch.</param>
/// <param name="PriceVolatility">Price volatility calculated from the batch.</param>
/// <param name="VolumeWeightedAveragePrice">VWAP calculated from the batch.</param>
/// <param name="PriceTrend">The price trend detected (Up, Down, Stable).</param>
/// <param name="AnomalyScore">Anomaly detection score (0-1, higher means more anomalous).</param>
/// <param name="ProcessingTimeMs">Time taken to process this batch in milliseconds.</param>
public record BatchAnalyticsResult(
    string BatchId,
    DateTime BatchTimestamp,
    string Symbol,
    int EventCount,
    decimal AveragePrice,
    decimal PriceVolatility,
    decimal VolumeWeightedAveragePrice,
    string PriceTrend,
    double AnomalyScore,
    long ProcessingTimeMs,
    IReadOnlyList<MarketDataEvent> OriginalEvents);

/// <summary>
///     Represents an individual alert event generated from batch analytics results.
///     This model demonstrates the output of unbatching - converting batch results back to individual events.
/// </summary>
/// <param name="AlertId">The unique identifier for this alert.</param>
/// <param name="Timestamp">When the alert was generated.</param>
/// <param name="SourceBatchId">The batch ID that generated this alert.</param>
/// <param name="Symbol">The trading symbol this alert relates to.</param>
/// <param name="OriginalEventId">The original market data event ID this alert is based on.</param>
/// <param name="AlertType">The type of alert (PriceAnomaly, VolumeSpike, TrendChange, etc.).</param>
/// <param name="Severity">The severity level of the alert (Low, Medium, High, Critical).</param>
/// <param name="Message">Descriptive message explaining the alert.</param>
/// <param name="TriggerValue">The value that triggered this alert.</param>
/// <param name="Threshold">The threshold that was exceeded.</param>
/// <param name="RequiresAction">Whether this alert requires immediate action.</param>
public record AlertEvent(
    string AlertId,
    DateTime Timestamp,
    string SourceBatchId,
    string Symbol,
    string OriginalEventId,
    string AlertType,
    string Severity,
    string Message,
    decimal TriggerValue,
    decimal Threshold,
    bool RequiresAction);