using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_CustomMergeNode.Models;

namespace Sample_CustomMergeNode.Nodes;

/// <summary>
///     Data quality validator for market data ticks
/// </summary>
public class DataQualityValidator : TransformNode<MarketDataTick, MarketDataTick>
{
    private readonly ILogger<DataQualityValidator>? _logger;
    private readonly Dictionary<string, DataQualityScore> _symbolQualityScores = new();

    /// <summary>
    ///     Initializes a new instance of the DataQualityValidator class
    /// </summary>
    /// <param name="logger">The logger</param>
    public DataQualityValidator(ILogger<DataQualityValidator>? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    ///     Validates and scores market data quality
    /// </summary>
    /// <param name="tick">The market data tick to validate</param>
    /// <param name="context">The pipeline context</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The validated market data tick with quality score</returns>
    public override async Task<MarketDataTick> ExecuteAsync(
        MarketDataTick tick,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (tick == null)
        {
            _logger?.LogWarning("Received null market data tick");
            throw new ArgumentNullException(nameof(tick));
        }

        try
        {
            // Calculate quality score
            var qualityScore = CalculateQualityScore(tick);

            // Update tick with quality score
            var validatedTick = new MarketDataTick(
                tick.Symbol,
                tick.Price,
                tick.Volume,
                tick.Timestamp,
                tick.Exchange,
                tick.Priority,
                qualityScore);

            // Log quality metrics
            _logger?.LogDebug(
                "Validated {Symbol} tick from {Exchange} - Quality Score: {QualityScore:F2}",
                validatedTick.Symbol,
                validatedTick.Exchange,
                qualityScore.OverallScore);

            // Simulate processing delay
            await Task.Delay(1, cancellationToken);

            return validatedTick;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error validating market data tick for {Symbol}", tick.Symbol);
            throw;
        }
    }

    /// <summary>
    ///     Calculates quality score for a market data tick
    /// </summary>
    /// <param name="tick">The market data tick</param>
    /// <returns>The quality score</returns>
    private DataQualityScore CalculateQualityScore(MarketDataTick tick)
    {
        // Get previous quality score for this symbol
        _symbolQualityScores.TryGetValue(tick.Symbol, out var previousScore);

        // Calculate completeness (based on required fields)
        var completeness = CalculateCompleteness(tick);

        // Calculate timeliness (based on timestamp freshness)
        var timeliness = CalculateTimeliness(tick);

        // Calculate accuracy (based on price and volume reasonableness)
        var accuracy = CalculateAccuracy(tick);

        // Calculate consistency (based on previous values)
        var consistency = CalculateConsistency(tick, previousScore);

        // Create quality score
        var qualityScore = new DataQualityScore(completeness, timeliness, accuracy, consistency);

        // Update symbol quality score
        _symbolQualityScores[tick.Symbol] = qualityScore;

        return qualityScore;
    }

    /// <summary>
    ///     Calculates completeness score based on required fields
    /// </summary>
    /// <param name="tick">The market data tick</param>
    /// <returns>Completeness score (0.0 to 1.0)</returns>
    private static double CalculateCompleteness(MarketDataTick tick)
    {
        var score = 1.0;

        // Check required fields
        if (string.IsNullOrWhiteSpace(tick.Symbol))
            score -= 0.3;

        if (tick.Price <= 0)
            score -= 0.3;

        if (tick.Volume <= 0)
            score -= 0.2;

        if (tick.Timestamp == default)
            score -= 0.2;

        return Math.Max(0.0, score);
    }

    /// <summary>
    ///     Calculates timeliness score based on timestamp freshness
    /// </summary>
    /// <param name="tick">The market data tick</param>
    /// <returns>Timeliness score (0.0 to 1.0)</returns>
    private static double CalculateTimeliness(MarketDataTick tick)
    {
        var now = DateTime.UtcNow;
        var age = now - tick.Timestamp;

        // Score based on age - newer is better
        return age.TotalSeconds switch
        {
            <= 1 => 1.0, // Very fresh
            <= 5 => 0.9, // Fresh
            <= 30 => 0.7, // Recent
            <= 300 => 0.5, // Within 5 minutes
            <= 3600 => 0.3, // Within 1 hour
            _ => 0.1, // Old data
        };
    }

    /// <summary>
    ///     Calculates accuracy score based on price and volume reasonableness
    /// </summary>
    /// <param name="tick">The market data tick</param>
    /// <returns>Accuracy score (0.0 to 1.0)</returns>
    private static double CalculateAccuracy(MarketDataTick tick)
    {
        var score = 1.0;

        // Check for reasonable price ranges (assuming major stock indices)
        if (tick.Price < 0)
            score -= 0.5;
        else if (tick.Price > 100000)
            score -= 0.3;

        // Check for reasonable volume
        if (tick.Volume < 0)
            score -= 0.3;
        else if (tick.Volume > 1000000000) // Very high volume
            score -= 0.1;

        return Math.Max(0.0, score);
    }

    /// <summary>
    ///     Calculates consistency score based on previous values
    /// </summary>
    /// <param name="tick">The market data tick</param>
    /// <param name="previousScore">Previous quality score for comparison</param>
    /// <returns>Consistency score (0.0 to 1.0)</returns>
    private static double CalculateConsistency(MarketDataTick tick, DataQualityScore? previousScore)
    {
        if (previousScore == null || tick.PreviousTick == null)
            return 0.8; // Neutral score for first tick

        var score = 1.0;

        // Check price consistency (shouldn't jump too dramatically)
        var priceChangeRatio = Math.Abs((tick.Price - tick.PreviousTick.Price) / tick.PreviousTick.Price);

        if (priceChangeRatio > 0.5m) // More than 50% change
            score -= 0.4;
        else if (priceChangeRatio > 0.2m) // More than 20% change
            score -= 0.2;
        else if (priceChangeRatio > 0.1m) // More than 10% change
            score -= 0.1;

        // Check timestamp consistency (should be sequential)
        if (tick.Timestamp <= tick.PreviousTick.Timestamp)
            score -= 0.3;

        return Math.Max(0.0, score);
    }
}
