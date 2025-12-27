using System;
using System.Collections.Generic;

namespace Sample_CustomMergeNode.Models;

/// <summary>
///     Represents merged market data from multiple exchanges
/// </summary>
public class MergedMarketData
{
    /// <summary>
    ///     Initializes a new instance of the MergedMarketData class
    /// </summary>
    /// <param name="symbol">The symbol of the financial instrument</param>
    /// <param name="mergedPrice">The merged price</param>
    /// <param name="mergedVolume">The merged volume</param>
    /// <param name="mergeTimestamp">The timestamp of the merge</param>
    /// <param name="sourceTicks">The source ticks that were merged</param>
    /// <param name="primaryExchange">The primary exchange used for the merge</param>
    /// <param name="qualityScore">The quality score of the merged data</param>
    /// <param name="mergeStrategy">The merge strategy used</param>
    /// <param name="conflictResolution">The conflict resolution method used</param>
    public MergedMarketData(
        string symbol,
        decimal mergedPrice,
        long mergedVolume,
        DateTime mergeTimestamp,
        IReadOnlyList<MarketDataTick> sourceTicks,
        string primaryExchange,
        DataQualityScore qualityScore,
        string mergeStrategy,
        string conflictResolution)
    {
        Symbol = symbol;
        MergedPrice = mergedPrice;
        MergedVolume = mergedVolume;
        MergeTimestamp = mergeTimestamp;
        SourceTicks = sourceTicks;
        PrimaryExchange = primaryExchange;
        QualityScore = qualityScore;
        MergeStrategy = mergeStrategy;
        ConflictResolution = conflictResolution;
    }

    /// <summary>
    ///     Gets the symbol of the financial instrument
    /// </summary>
    public string Symbol { get; }

    /// <summary>
    ///     Gets the merged price
    /// </summary>
    public decimal MergedPrice { get; }

    /// <summary>
    ///     Gets the merged volume
    /// </summary>
    public long MergedVolume { get; }

    /// <summary>
    ///     Gets the timestamp of the merge
    /// </summary>
    public DateTime MergeTimestamp { get; }

    /// <summary>
    ///     Gets the source ticks that were merged
    /// </summary>
    public IReadOnlyList<MarketDataTick> SourceTicks { get; }

    /// <summary>
    ///     Gets the primary exchange used for the merge
    /// </summary>
    public string PrimaryExchange { get; }

    /// <summary>
    ///     Gets the quality score of the merged data
    /// </summary>
    public DataQualityScore QualityScore { get; }

    /// <summary>
    ///     Gets the merge strategy used
    /// </summary>
    public string MergeStrategy { get; }

    /// <summary>
    ///     Gets the conflict resolution method used
    /// </summary>
    public string ConflictResolution { get; }

    /// <summary>
    ///     Returns a string representation of the merged market data
    /// </summary>
    /// <returns>String representation</returns>
    public override string ToString()
    {
        return
            $"{Symbol}: ${MergedPrice:F2} ({MergedVolume:N0} shares) at {MergeTimestamp:HH:mm:ss.fff} from {PrimaryExchange} [{MergeStrategy}] {QualityScore}";
    }
}
