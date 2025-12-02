using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_StreamingAnalytics.Nodes;

/// <summary>
///     Source node that generates time-series data with realistic timing patterns.
///     This node simulates real-time data streams with configurable frequency and late data.
/// </summary>
public class TimeSeriesSource : SourceNode<TimeSeriesData>
{
    private readonly Random _random = new();
    private readonly string[] _sources = { "Sensor-A", "Sensor-B", "Sensor-C", "Sensor-D" };

    /// <summary>
    ///     Generates a stream of time-series data points with realistic timing.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated time-series data.</returns>
    public override IDataPipe<TimeSeriesData> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting TimeSeriesSource - generating time-series data stream");

        return new StreamingDataPipe<TimeSeriesData>(GenerateTimeSeriesDataAsync(cancellationToken), "TimeSeriesSource");
    }

    /// <summary>
    ///     Generates time-series data asynchronously with realistic timing patterns.
    /// </summary>
    private async IAsyncEnumerable<TimeSeriesData> GenerateTimeSeriesDataAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        var dataPointCount = 0;

        // Generate data for approximately 30 seconds of real-time processing
        while (DateTime.UtcNow - startTime < TimeSpan.FromSeconds(30) && !cancellationToken.IsCancellationRequested)
        {
            dataPointCount++;

            // Generate data point with current timestamp
            var timestamp = DateTime.UtcNow;
            var source = _sources[_random.Next(_sources.Length)];
            var value = GenerateRealisticValue(timestamp, source);

            var dataPoint = new TimeSeriesData
            {
                Timestamp = timestamp,
                Value = value,
                Source = source,
                IsLate = false,
            };

            yield return dataPoint;

            // Occasionally generate late-arriving data (10% chance)
            if (_random.NextDouble() < 0.1)
            {
                var lateTimestamp = timestamp.AddSeconds(-_random.Next(5, 30)); // 5-30 seconds late
                var lateValue = GenerateRealisticValue(lateTimestamp, source);

                var lateDataPoint = new TimeSeriesData
                {
                    Timestamp = DateTime.UtcNow, // Current processing time
                    Value = lateValue,
                    Source = source,
                    IsLate = true,
                    OriginalTimestamp = lateTimestamp,
                };

                yield return lateDataPoint;

                Console.WriteLine($"Generated late data point: {lateDataPoint}");
            }

            // Variable delay between data points (100-500ms) to simulate realistic timing
            var delay = TimeSpan.FromMilliseconds(_random.Next(100, 500));
            await Task.Delay(delay, cancellationToken);
        }

        Console.WriteLine($"TimeSeriesSource completed - generated {dataPointCount} data points");
    }

    /// <summary>
    ///     Generates a realistic value based on timestamp and source.
    ///     Creates patterns that simulate sensor readings with daily cycles and noise.
    /// </summary>
    private double GenerateRealisticValue(DateTime timestamp, string source)
    {
        // Base value varies by source
        var baseValue = source switch
        {
            "Sensor-A" => 50.0,
            "Sensor-B" => 75.0,
            "Sensor-C" => 100.0,
            "Sensor-D" => 25.0,
            _ => 60.0,
        };

        // Add daily cycle (sinusoidal pattern)
        var hours = timestamp.TimeOfDay.TotalHours;
        var cycleFactor = Math.Sin((hours - 6) * Math.PI / 12); // Peak at noon, trough at midnight
        var cycleValue = baseValue * 0.3 * cycleFactor;

        // Add random noise
        var noise = (_random.NextDouble() - 0.5) * baseValue * 0.1;

        // Combine all factors
        return baseValue + cycleValue + noise;
    }
}
