namespace Sample_CustomMergeNode.Models;

/// <summary>
///     Represents the priority of an exchange for market data merging
/// </summary>
public enum ExchangePriority
{
    /// <summary>
    ///     Lowest priority (e.g., international exchanges with higher latency)
    /// </summary>
    Low = 1,

    /// <summary>
    ///     Medium priority (e.g., regional exchanges)
    /// </summary>
    Medium = 2,

    /// <summary>
    ///     High priority (e.g., major national exchanges like NASDAQ)
    /// </summary>
    High = 3,

    /// <summary>
    ///     Highest priority (e.g., primary exchanges like NYSE with most reliable data)
    /// </summary>
    Critical = 4,
}
