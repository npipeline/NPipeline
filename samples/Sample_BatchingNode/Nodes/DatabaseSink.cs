using System;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BatchingNode.Nodes;

/// <summary>
///     Sink node that simulates bulk database operations for batched results.
///     This node demonstrates how batching improves database operation efficiency.
/// </summary>
public class DatabaseSink : SinkNode<BatchProcessingResult>
{
    private readonly double _failureRate;
    private readonly bool _simulateDatabaseDelay;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DatabaseSink" /> class.
    /// </summary>
    /// <param name="simulateDatabaseDelay">Whether to simulate database operation delay.</param>
    /// <param name="failureRate">Simulated failure rate (0.0 to 1.0).</param>
    public DatabaseSink(bool simulateDatabaseDelay = true, double failureRate = 0.0)
    {
        _simulateDatabaseDelay = simulateDatabaseDelay;
        _failureRate = Math.Clamp(failureRate, 0.0, 1.0);
    }

    /// <summary>
    ///     Processes batch processing results by simulating bulk database inserts.
    ///     This demonstrates the efficiency benefits of batched database operations.
    /// </summary>
    /// <param name="input">The data pipe containing batch processing results.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<BatchProcessingResult> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting bulk database operations for batched results");
        Console.WriteLine();

        var totalBatches = 0;
        var totalReadings = 0;
        var successfulInserts = 0;
        var failedInserts = 0;
        var totalProcessingTime = 0L;
        var totalDatabaseTime = 0L;

        var random = new Random(42); // Fixed seed for reproducible results

        // Print header for batch results
        Console.WriteLine("=== BATCH DATABASE OPERATIONS ===");
        Console.WriteLine();
        Console.WriteLine("Batch ID | Device    | Readings | Avg Temp | Proc Time | DB Time | Status");
        Console.WriteLine("---------|-----------|----------|----------|-----------|---------|--------");

        // Use await foreach to consume all batch results from the input pipe
        await foreach (var batchResult in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            totalBatches++;
            totalReadings += batchResult.ReadingCount;
            totalProcessingTime += batchResult.ProcessingTimeMs;

            var databaseStartTime = DateTime.UtcNow;

            // Simulate random database failures based on failure rate
            var shouldFail = _failureRate > 0 && random.NextDouble() < _failureRate;

            try
            {
                if (_simulateDatabaseDelay)
                {
                    // Simulate database operation time (much faster for batches than individual inserts)
                    // Individual inserts would be ~5ms per record, batch insert is ~10ms + 1ms per record
                    var delay = 10 + batchResult.ReadingCount;
                    await Task.Delay(delay, cancellationToken);
                }

                var databaseEndTime = DateTime.UtcNow;
                var databaseTimeMs = (long)(databaseEndTime - databaseStartTime).TotalMilliseconds;
                totalDatabaseTime += databaseTimeMs;

                if (shouldFail)
                    throw new InvalidOperationException("Simulated database constraint violation");

                successfulInserts++;

                // Format and display the batch result
                var batchId = batchResult.BatchId.PadRight(8);
                var deviceId = batchResult.DeviceId.PadRight(9);
                var readings = batchResult.ReadingCount.ToString().PadRight(8);
                var avgTemp = $"{batchResult.AverageTemperature:F1}°C".PadRight(8);
                var procTime = $"{batchResult.ProcessingTimeMs}ms".PadRight(7);
                var dbTime = $"{databaseTimeMs}ms".PadRight(6);
                var status = "SUCCESS";

                Console.WriteLine($"{batchId} | {deviceId} | {readings} | {avgTemp} | {procTime} | {dbTime} | {status}");
            }
            catch (Exception ex)
            {
                failedInserts++;
                var databaseTimeMs = (long)(DateTime.UtcNow - databaseStartTime).TotalMilliseconds;
                totalDatabaseTime += databaseTimeMs;

                // Format and display the failed batch result
                var batchId = batchResult.BatchId.PadRight(8);
                var deviceId = batchResult.DeviceId.PadRight(9);
                var readings = batchResult.ReadingCount.ToString().PadRight(8);
                var avgTemp = $"{batchResult.AverageTemperature:F1}°C".PadRight(8);
                var procTime = $"{batchResult.ProcessingTimeMs}ms".PadRight(7);
                var dbTime = $"{databaseTimeMs}ms".PadRight(6);
                var status = "FAILED";

                Console.WriteLine($"{batchId} | {deviceId} | {readings} | {avgTemp} | {procTime} | {dbTime} | {status}");
                Console.WriteLine($"         Error: {ex.Message}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("=== DATABASE OPERATIONS SUMMARY ===");
        Console.WriteLine($"Total batches processed: {totalBatches}");
        Console.WriteLine($"Total sensor readings: {totalReadings}");
        Console.WriteLine($"Successful database inserts: {successfulInserts}");
        Console.WriteLine($"Failed database inserts: {failedInserts}");
        Console.WriteLine($"Success rate: {(totalBatches > 0 ? successfulInserts * 100.0 / totalBatches : 0):F1}%");
        Console.WriteLine();
        Console.WriteLine("=== PERFORMANCE ANALYSIS ===");
        Console.WriteLine($"Total batch processing time: {totalProcessingTime}ms");
        Console.WriteLine($"Total database operation time: {totalDatabaseTime}ms");
        Console.WriteLine($"Average processing time per batch: {(totalBatches > 0 ? totalProcessingTime / totalBatches : 0):F1}ms");
        Console.WriteLine($"Average database time per batch: {(totalBatches > 0 ? totalDatabaseTime / totalBatches : 0):F1}ms");

        if (totalReadings > 0)
        {
            Console.WriteLine($"Average time per reading (including processing): {(totalProcessingTime + totalDatabaseTime) / (double)totalReadings:F2}ms");
            Console.WriteLine($"Estimated time for individual inserts: {totalReadings * 5}ms (5ms per reading)");

            Console.WriteLine(
                $"Time saved by batching: {Math.Max(0, totalReadings * 5 - (totalProcessingTime + totalDatabaseTime))}ms ({Math.Max(0, (1.0 - (totalProcessingTime + totalDatabaseTime) / (double)(totalReadings * 5)) * 100):F1}% improvement)");
        }

        Console.WriteLine();
        Console.WriteLine("DatabaseSink completed processing all batch results.");
    }
}
