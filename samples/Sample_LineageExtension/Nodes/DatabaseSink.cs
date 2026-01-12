using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Sink node that simulates writing processed orders to a database.
///     Demonstrates how lineage information can be used for auditing and debugging.
/// </summary>
public class DatabaseSink : SinkNode<ProcessedOrder>
{
    private readonly string _databaseName;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DatabaseSink" /> class.
    /// </summary>
    /// <param name="databaseName">The name of the simulated database.</param>
    public DatabaseSink(string databaseName = "OrdersDB")
    {
        _databaseName = databaseName;
    }

    /// <summary>
    ///     Simulates writing processed orders to a database.
    /// </summary>
    /// <param name="input">The input data pipe containing processed orders.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    public override async Task ExecuteAsync(IDataPipe<ProcessedOrder> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[DatabaseSink] Connecting to database '{_databaseName}'...");

        // Simulate database connection delay
        await Task.Delay(100, cancellationToken);

        Console.WriteLine($"[DatabaseSink] Connected to database '{_databaseName}'");
        Console.WriteLine($"[DatabaseSink] Starting to insert processed orders...");

        var count = 0;
        var successCount = 0;
        var failedCount = 0;

        await foreach (var processedOrder in input.WithCancellation(cancellationToken))
        {
            count++;

            try
            {
                // Simulate database insert operation
                await SimulateInsertAsync(processedOrder, cancellationToken);

                if (processedOrder.Result == ProcessingResult.Success)
                {
                    successCount++;
                }
                else
                {
                    failedCount++;
                }

                // Simulate variable processing time
                await Task.Delay(random.Next(10, 50), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DatabaseSink] Error inserting order #{processedOrder.ValidatedOrder.EnrichedOrder.Order.OrderId}: {ex.Message}");
                failedCount++;
            }
        }

        Console.WriteLine($"[DatabaseSink] Finished inserting {count} processed orders into '{_databaseName}':");
        Console.WriteLine($"  Successful: {successCount}");
        Console.WriteLine($"  Failed/Rejected: {failedCount}");
        Console.WriteLine($"[DatabaseSink] Closing database connection...");

        // Simulate connection close delay
        await Task.Delay(50, cancellationToken);

        Console.WriteLine($"[DatabaseSink] Database connection closed");
    }

    /// <summary>
    ///     Simulates inserting a processed order into the database.
    /// </summary>
    private static async Task SimulateInsertAsync(ProcessedOrder processedOrder, CancellationToken cancellationToken)
    {
        var order = processedOrder.ValidatedOrder.EnrichedOrder.Order;
        var customer = processedOrder.ValidatedOrder.EnrichedOrder.Customer;

        // Simulate SQL INSERT statement
        var sql = $"""
            INSERT INTO ProcessedOrders (
                OrderId, CustomerId, CustomerName, TotalAmount, 
                Discount, FinalAmount, ProcessingResult, ProcessedAt
            ) VALUES (
                {order.OrderId}, {customer.CustomerId}, '{customer.FullName}', 
                {order.TotalAmount}, {processedOrder.ValidatedOrder.EnrichedOrder.Discount}, 
                {processedOrder.ValidatedOrder.EnrichedOrder.FinalAmount}, 
                '{processedOrder.Result}', '{processedOrder.ProcessedAt:yyyy-MM-dd HH:mm:ss}'
            );
            """;

        // In a real implementation, this would execute against an actual database
        // For demonstration, we just simulate the operation
        await Task.CompletedTask;
    }

    private static readonly Random random = new();
}