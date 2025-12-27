using System;
using System.Collections.Generic;

namespace Sample_CustomMergeNode.Models;

/// <summary>
///     Represents a single market data tick from an exchange
/// </summary>
public class MarketDataTick
{
    /// <summary>
    ///     Initializes a new instance of the MarketDataTick class
    /// </summary>
    /// <param name="symbol">The symbol of the financial instrument</param>
    /// <param name="price">The price of the instrument</param>
    /// <param name="volume">The volume of the trade</param>
    /// <param name="timestamp">The timestamp of the tick</param>
    /// <param name="exchange">The name of the exchange</param>
    /// <param name="priority">The priority of the exchange</param>
    /// <param name="qualityScore">The quality score of the tick</param>
    public MarketDataTick(
        string symbol,
        decimal price,
        long volume,
        DateTime timestamp,
        string exchange,
        ExchangePriority priority,
        DataQualityScore? qualityScore = null)
    {
        Symbol = symbol;
        Price = price;
        Volume = volume;
        Timestamp = timestamp;
        Exchange = exchange;
        Priority = priority;
        QualityScore = qualityScore;
        QualityIndicators = new DataQualityIndicators();
    }

    /// <summary>
    ///     Gets the symbol of the financial instrument
    /// </summary>
    public string Symbol { get; }

    /// <summary>
    ///     Gets the price of the instrument
    /// </summary>
    public decimal Price { get; }

    /// <summary>
    ///     Gets the volume of the trade
    /// </summary>
    public long Volume { get; }

    /// <summary>
    ///     Gets the timestamp of the tick
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Gets the name of the exchange
    /// </summary>
    public string Exchange { get; }

    /// <summary>
    ///     Gets the priority of the exchange
    /// </summary>
    public ExchangePriority Priority { get; }

    /// <summary>
    ///     Gets the quality score of the tick
    /// </summary>
    public DataQualityScore? QualityScore { get; set; }

    /// <summary>
    ///     Gets or sets the previous tick for comparison
    /// </summary>
    public MarketDataTick? PreviousTick { get; set; }

    /// <summary>
    ///     Gets the data quality indicators
    /// </summary>
    public DataQualityIndicators QualityIndicators { get; }

    /// <summary>
    ///     Returns a string representation of the market data tick
    /// </summary>
    /// <returns>String representation</returns>
    public override string ToString()
    {
        return $"{Symbol}: ${Price:F2} ({Volume:N0} shares) at {Timestamp:HH:mm:ss.fff} from {Exchange} [{Priority}] {QualityScore}";
    }
}

/// <summary>
///     Represents data quality indicators for market data
/// </summary>
public class DataQualityIndicators
{
    /// <summary>
    ///     Initializes a new instance of the DataQualityIndicators class
    /// </summary>
    public DataQualityIndicators()
    {
        IsStale = false;
        HasGaps = false;
        IsSuspiciousPrice = false;
        IsSuspiciousVolume = false;
        IsOutOfOrder = false;
        IsDuplicate = false;
        IsIncomplete = false;
        IsInconsistent = false;
        IsDelayed = false;
        HasErrors = false;
    }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is stale
    /// </summary>
    public bool IsStale { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data has gaps
    /// </summary>
    public bool HasGaps { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the price is suspicious
    /// </summary>
    public bool IsSuspiciousPrice { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the volume is suspicious
    /// </summary>
    public bool IsSuspiciousVolume { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is out of order
    /// </summary>
    public bool IsOutOfOrder { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is a duplicate
    /// </summary>
    public bool IsDuplicate { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is incomplete
    /// </summary>
    public bool IsIncomplete { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is inconsistent
    /// </summary>
    public bool IsInconsistent { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is delayed
    /// </summary>
    public bool IsDelayed { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data has errors
    /// </summary>
    public bool HasErrors { get; set; }

    /// <summary>
    ///     Returns a string representation of the data quality indicators
    /// </summary>
    /// <returns>String representation</returns>
    public override string ToString()
    {
        var issues = new List<string>();

        if (IsStale)
            issues.Add("Stale");

        if (HasGaps)
            issues.Add("Gaps");

        if (IsSuspiciousPrice)
            issues.Add("SuspiciousPrice");

        if (IsSuspiciousVolume)
            issues.Add("SuspiciousVolume");

        if (IsOutOfOrder)
            issues.Add("OutOfOrder");

        if (IsDuplicate)
            issues.Add("Duplicate");

        if (IsIncomplete)
            issues.Add("Incomplete");

        if (IsInconsistent)
            issues.Add("Inconsistent");

        if (IsDelayed)
            issues.Add("Delayed");

        if (HasErrors)
            issues.Add("Errors");

        return issues.Count > 0
            ? string.Join(", ", issues)
            : "Good";
    }
}
