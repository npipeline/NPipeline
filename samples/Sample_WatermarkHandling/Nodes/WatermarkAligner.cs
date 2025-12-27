using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Nodes;

/// <summary>
///     Transform node that synchronizes and aligns watermarks across multiple sensor streams.
///     This node demonstrates multi-stream watermark coordination and temporal alignment strategies.
/// </summary>
public class WatermarkAligner : TransformNode<SensorReading, SensorReading>
{
    private readonly object _alignmentLock = new();
    private readonly WatermarkMetrics _globalMetrics = new();
    private readonly DateTimeOffset _globalWatermark = DateTimeOffset.MinValue;
    private readonly ILogger<WatermarkAligner> _logger;
    private readonly ConcurrentDictionary<string, StreamWatermarkState> _streamStates = new();
    private DateTimeOffset _lastAlignmentTime = DateTimeOffset.MinValue;

    /// <summary>
    ///     Initializes a new instance of the WatermarkAligner class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public WatermarkAligner(ILogger<WatermarkAligner> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    ///     Processes sensor reading and aligns watermarks across multiple streams.
    /// </summary>
    /// <param name="reading">The input sensor reading.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The processed sensor reading with aligned watermark information.</returns>
    public override async Task<SensorReading> ExecuteAsync(
        SensorReading reading,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Processing reading from {DeviceId} for watermark alignment",
            reading.DeviceId);

        // Update stream state for this device
        UpdateStreamState(reading.DeviceId, reading.Timestamp);

        // Calculate aligned watermark
        var alignedWatermark = await CalculateAlignedWatermark(cancellationToken);

        // Check for late data
        if (IsLateData(reading, alignedWatermark))
            await HandleLateData(reading, alignedWatermark, cancellationToken);

        // Update quality indicators with alignment information
        var updatedQualityIndicators = UpdateQualityIndicators(
            reading.QualityIndicators, alignedWatermark, reading.Timestamp);

        var processedReading = reading with { QualityIndicators = updatedQualityIndicators };

        _logger.LogDebug("Processed reading from {DeviceId}, aligned watermark: {Watermark}",
            reading.DeviceId, alignedWatermark);

        return processedReading;
    }

    /// <summary>
    ///     Updates stream state for a specific device.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    /// <param name="timestamp">The event timestamp.</param>
    private void UpdateStreamState(string deviceId, DateTimeOffset timestamp)
    {
        _streamStates.AddOrUpdate(deviceId, new StreamWatermarkState
        {
            DeviceId = deviceId,
            LastEventTimestamp = timestamp,
            LastWatermark = timestamp,
            EventCount = 1,
        }, (_, existing) =>
        {
            existing.LastEventTimestamp = timestamp;
            existing.LastWatermark = timestamp;
            existing.EventCount++;
            return existing;
        });
    }

    /// <summary>
    ///     Calculates aligned watermark across all streams.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The aligned watermark.</returns>
    private async Task<DateTimeOffset> CalculateAlignedWatermark(CancellationToken cancellationToken)
    {
        return await Task.Run(() =>
        {
            lock (_alignmentLock)
            {
                if (_streamStates.IsEmpty)
                    return DateTimeOffset.MinValue;

                // Get all current watermarks
                var watermarks = _streamStates.Values.Select(s => s.LastWatermark).ToList();

                if (watermarks.Count == 0)
                    return DateTimeOffset.MinValue;

                // Calculate alignment strategy based on stream characteristics
                var alignmentStrategy = DetermineAlignmentStrategy(watermarks);

                return alignmentStrategy switch
                {
                    WatermarkAlignmentStrategy.Minimum => watermarks.Min(),
                    WatermarkAlignmentStrategy.Maximum => watermarks.Max(),
                    WatermarkAlignmentStrategy.Average => CalculateAverageWatermark(watermarks),
                    WatermarkAlignmentStrategy.Median => CalculateMedianWatermark(watermarks),
                    WatermarkAlignmentStrategy.WeightedAverage => CalculateWeightedAverageWatermark(watermarks),
                    _ => watermarks.Min(), // Default to minimum for safety
                };
            }
        }, cancellationToken);
    }

    /// <summary>
    ///     Determines the alignment strategy based on stream characteristics.
    /// </summary>
    /// <param name="watermarks">The current watermarks from all streams.</param>
    /// <returns>The alignment strategy to use.</returns>
    private WatermarkAlignmentStrategy DetermineAlignmentStrategy(List<DateTimeOffset> watermarks)
    {
        var spread = watermarks.Max() - watermarks.Min();
        var count = watermarks.Count;

        // Use different strategies based on conditions
        if (spread > TimeSpan.FromSeconds(5))
            return WatermarkAlignmentStrategy.Minimum; // High spread - be conservative

        if (count > 10)
            return WatermarkAlignmentStrategy.WeightedAverage; // Many streams - use weighted average

        if (spread < TimeSpan.FromMilliseconds(100))
            return WatermarkAlignmentStrategy.Average; // Low spread - use average

        return WatermarkAlignmentStrategy.Median; // Default to median
    }

    /// <summary>
    ///     Calculates average watermark.
    /// </summary>
    /// <param name="watermarks">The watermarks to average.</param>
    /// <returns>The average watermark.</returns>
    private static DateTimeOffset CalculateAverageWatermark(List<DateTimeOffset> watermarks)
    {
        var totalTicks = watermarks.Sum(w => w.Ticks);
        return new DateTimeOffset(totalTicks / watermarks.Count, TimeSpan.Zero);
    }

    /// <summary>
    ///     Calculates median watermark.
    /// </summary>
    /// <param name="watermarks">The watermarks to find median for.</param>
    /// <returns>The median watermark.</returns>
    private static DateTimeOffset CalculateMedianWatermark(List<DateTimeOffset> watermarks)
    {
        var sorted = watermarks.OrderBy(w => w).ToList();
        var middle = sorted.Count / 2;

        return sorted.Count % 2 == 0
            ? new DateTimeOffset((sorted[middle - 1].Ticks + sorted[middle].Ticks) / 2, TimeSpan.Zero)
            : sorted[middle];
    }

    /// <summary>
    ///     Calculates weighted average watermark.
    /// </summary>
    /// <param name="watermarks">The watermarks to average.</param>
    /// <returns>The weighted average watermark.</returns>
    private static DateTimeOffset CalculateWeightedAverageWatermark(List<DateTimeOffset> watermarks)
    {
        // Simulate weighting based on device reliability and event count
        var weightedSum = 0L;
        var totalWeight = 0L;

        foreach (var watermark in watermarks)
        {
            var weight = GetDeviceWeight(watermark);

            try
            {
                checked
                {
                    weightedSum += watermark.Ticks * weight;
                    totalWeight += weight;
                }
            }
            catch (OverflowException)
            {
                // Handle overflow by using double arithmetic
                var weightedSumDouble = weightedSum + watermark.Ticks * (double)weight;
                var totalWeightDouble = (double)totalWeight + weight;
                var averageTicks = weightedSumDouble / totalWeightDouble;
                return new DateTimeOffset((long)averageTicks, TimeSpan.Zero);
            }
        }

        return totalWeight > 0
            ? new DateTimeOffset(weightedSum / totalWeight, TimeSpan.Zero)
            : watermarks.Min();
    }

    /// <summary>
    ///     Gets device weight for watermark calculation.
    /// </summary>
    /// <param name="watermark">The device watermark.</param>
    /// <returns>The device weight.</returns>
    private static int GetDeviceWeight(DateTimeOffset watermark)
    {
        // Simulate device reliability weighting
        // More recent events get higher weight
        var age = DateTimeOffset.UtcNow - watermark;

        return age < TimeSpan.FromMinutes(5)
            ? 3
            : age < TimeSpan.FromMinutes(15)
                ? 2
                : 1;
    }

    /// <summary>
    ///     Checks if data is late relative to aligned watermark.
    /// </summary>
    /// <param name="reading">The sensor reading.</param>
    /// <param name="alignedWatermark">The aligned watermark.</param>
    /// <returns>True if data is late, false otherwise.</returns>
    private static bool IsLateData(SensorReading reading, DateTimeOffset alignedWatermark)
    {
        return reading.Timestamp < alignedWatermark;
    }

    /// <summary>
    ///     Handles late data by creating records and updating statistics.
    /// </summary>
    /// <param name="reading">The late sensor reading.</param>
    /// <param name="alignedWatermark">The current aligned watermark.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task HandleLateData(
        SensorReading reading,
        DateTimeOffset alignedWatermark,
        CancellationToken cancellationToken)
    {
        var lateBy = alignedWatermark - reading.Timestamp;

        // Create late data record
        var lateRecord = new LateDataRecord(
            reading.DeviceId,
            reading.Timestamp.UtcDateTime,
            DateTimeOffset.UtcNow.UtcDateTime,
            LateDataHandlingAction.SideOutput,
            $"Event is {lateBy.TotalMilliseconds:F0}ms late");

        // Update late data statistics
        _globalMetrics.LateDataCount++;
        _globalMetrics.LastUpdated = DateTimeOffset.UtcNow;

        // Log late data detection
        _logger.LogInformation(
            "Late data detected: {DeviceId} - Timestamp: {Timestamp:HH:mm:ss.fff}, Late by: {LatenessMs:F0}ms",
            reading.DeviceId,
            reading.Timestamp,
            lateBy.TotalMilliseconds);

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Updates quality indicators with alignment information.
    /// </summary>
    /// <param name="originalIndicators">The original quality indicators.</param>
    /// <param name="alignedWatermark">The aligned watermark.</param>
    /// <param name="eventTime">The event timestamp.</param>
    /// <returns>Updated quality indicators with alignment information.</returns>
    private static DataQualityIndicators UpdateQualityIndicators(
        DataQualityIndicators originalIndicators,
        DateTimeOffset alignedWatermark,
        DateTimeOffset eventTime)
    {
        var alignmentDelay = alignedWatermark - eventTime;

        return new DataQualityIndicators
        {
            CompletenessScore = originalIndicators.CompletenessScore,
            TimelinessScore = originalIndicators.TimelinessScore,
            AccuracyScore = originalIndicators.AccuracyScore,
            ConsistencyScore = originalIndicators.ConsistencyScore,
            IsStale = originalIndicators.IsStale,
            HasGaps = originalIndicators.HasGaps,
            IsSuspiciousValue = originalIndicators.IsSuspiciousValue,
            IsOutOfOrder = originalIndicators.IsOutOfOrder || alignmentDelay > TimeSpan.Zero,
            IsDuplicate = originalIndicators.IsDuplicate,
            IsIncomplete = originalIndicators.IsIncomplete,
            IsInconsistent = originalIndicators.IsInconsistent,
            IsDelayed = originalIndicators.IsDelayed || alignmentDelay > TimeSpan.FromMilliseconds(100),
            HasErrors = originalIndicators.HasErrors,
        };
    }

    /// <summary>
    ///     Gets current watermark alignment metrics.
    /// </summary>
    /// <returns>Watermark alignment metrics.</returns>
    public WatermarkAlignmentMetrics GetAlignmentMetrics()
    {
        lock (_alignmentLock)
        {
            if (_streamStates.IsEmpty)
                return new WatermarkAlignmentMetrics();

            var watermarks = _streamStates.Values.Select(s => s.LastWatermark).ToList();
            var minWatermark = watermarks.Min();
            var maxWatermark = watermarks.Max();
            var averageTicks = watermarks.Average(w => w.Ticks);

            // Calculate watermark statistics
            var stats = new WatermarkStatistics
            {
                MinimumWatermark = minWatermark,
                MaximumWatermark = maxWatermark,
                AverageWatermark = new DateTimeOffset((long)averageTicks, TimeSpan.Zero),
                WatermarkSpread = maxWatermark - minWatermark,
                StreamCount = _streamStates.Count,
                LastUpdated = DateTimeOffset.UtcNow,
            };

            // Log alignment results
            _logger.LogInformation(
                "Watermark alignment completed: Min={MinWatermark:HH:mm:ss.fff}, Max={MaxWatermark:HH:mm:ss.fff}, Spread={SpreadMs:F0}ms, Aligned={AlignedWatermark:HH:mm:ss.fff}",
                minWatermark,
                maxWatermark,
                stats.WatermarkSpread.TotalMilliseconds,
                _globalWatermark);

            // Update global watermark tracking
            _lastAlignmentTime = DateTimeOffset.UtcNow;

            // Update stream states with aligned watermark
            foreach (var state in _streamStates.Values)
            {
                state.LastAlignedWatermark = _globalWatermark;
                state.AlignmentCount++;
            }

            return new WatermarkAlignmentMetrics
            {
                AlignedWatermark = _globalWatermark,
                AlignmentStrategy = DetermineAlignmentStrategy(watermarks),
                StreamCount = _streamStates.Count,
                WatermarkSpread = stats.WatermarkSpread,
                AlignmentQuality = CalculateAlignmentQuality(stats),
                LastAlignmentTime = _lastAlignmentTime,
                TotalAlignments = _streamStates.Values.Sum(s => s.AlignmentCount),
            };
        }
    }

    /// <summary>
    ///     Calculates alignment quality based on watermark statistics.
    /// </summary>
    /// <param name="stats">The watermark statistics.</param>
    /// <returns>Alignment quality score.</returns>
    private static double CalculateAlignmentQuality(WatermarkStatistics stats)
    {
        // Calculate quality based on spread and consistency
        var spreadPenalty = Math.Min(1.0, stats.WatermarkSpread.TotalMilliseconds / 1000.0);

        var consistencyBonus = stats.StreamCount > 5
            ? 0.1
            : 0.0;

        return Math.Max(0.0, 1.0 - spreadPenalty + consistencyBonus);
    }

    /// <summary>
    ///     Creates a late data record for out-of-order events.
    /// </summary>
    /// <param name="reading">The late sensor reading.</param>
    /// <param name="alignedWatermark">The current aligned watermark.</param>
    /// <returns>A late data record.</returns>
    private static LateDataRecord CreateLateDataRecord(SensorReading reading, DateTimeOffset alignedWatermark)
    {
        return new LateDataRecord(
            reading.DeviceId,
            reading.Timestamp.UtcDateTime,
            DateTimeOffset.UtcNow.UtcDateTime,
            LateDataHandlingAction.SideOutput,
            "Event arrived after watermark advancement");
    }
}

/// <summary>
///     Represents watermark statistics for alignment.
/// </summary>
public class WatermarkStatistics
{
    /// <summary>
    ///     Gets or sets the minimum watermark.
    /// </summary>
    public DateTimeOffset MinimumWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the maximum watermark.
    /// </summary>
    public DateTimeOffset MaximumWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the average watermark.
    /// </summary>
    public DateTimeOffset AverageWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the watermark spread.
    /// </summary>
    public TimeSpan WatermarkSpread { get; set; }

    /// <summary>
    ///     Gets or sets the stream count.
    /// </summary>
    public int StreamCount { get; set; }

    /// <summary>
    ///     Gets or sets the last updated time.
    /// </summary>
    public DateTimeOffset LastUpdated { get; set; }
}

/// <summary>
///     Represents watermark alignment metrics.
/// </summary>
public class WatermarkAlignmentMetrics
{
    /// <summary>
    ///     Gets or sets the aligned watermark.
    /// </summary>
    public DateTimeOffset AlignedWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the alignment strategy.
    /// </summary>
    public WatermarkAlignmentStrategy AlignmentStrategy { get; set; }

    /// <summary>
    ///     Gets or sets the stream count.
    /// </summary>
    public int StreamCount { get; set; }

    /// <summary>
    ///     Gets or sets the watermark spread.
    /// </summary>
    public TimeSpan WatermarkSpread { get; set; }

    /// <summary>
    ///     Gets or sets the alignment quality.
    /// </summary>
    public double AlignmentQuality { get; set; }

    /// <summary>
    ///     Gets or sets the last alignment time.
    /// </summary>
    public DateTimeOffset LastAlignmentTime { get; set; }

    /// <summary>
    ///     Gets or sets the total alignments.
    /// </summary>
    public int TotalAlignments { get; set; }
}

/// <summary>
///     Represents the state of a stream's watermark.
/// </summary>
public class StreamWatermarkState
{
    /// <summary>
    ///     Gets or sets the device identifier.
    /// </summary>
    public string DeviceId { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the last event timestamp.
    /// </summary>
    public DateTimeOffset LastEventTimestamp { get; set; }

    /// <summary>
    ///     Gets or sets the last watermark.
    /// </summary>
    public DateTimeOffset LastWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the event count.
    /// </summary>
    public int EventCount { get; set; }

    /// <summary>
    ///     Gets or sets the last aligned watermark.
    /// </summary>
    public DateTimeOffset LastAlignedWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the alignment count.
    /// </summary>
    public int AlignmentCount { get; set; }
}

/// <summary>
///     Represents watermark alignment strategies.
/// </summary>
public enum WatermarkAlignmentStrategy
{
    /// <summary>
    ///     Use the minimum watermark across all streams.
    /// </summary>
    Minimum,

    /// <summary>
    ///     Use the maximum watermark across all streams.
    /// </summary>
    Maximum,

    /// <summary>
    ///     Use the average watermark across all streams.
    /// </summary>
    Average,

    /// <summary>
    ///     Use the median watermark across all streams.
    /// </summary>
    Median,

    /// <summary>
    ///     Use a weighted average watermark across all streams.
    /// </summary>
    WeightedAverage,
}
