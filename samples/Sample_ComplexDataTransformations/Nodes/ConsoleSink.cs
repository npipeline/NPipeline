using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Sink node that outputs pipeline results to the console.
///     This node demonstrates how to create a sink that processes and displays
///     the final results of the complex data transformations pipeline.
/// </summary>
public class ConsoleSink : SinkNode<object>
{
    private int _processedCount;

    /// <summary>
    ///     Processes items from the data pipe by outputting them to the console.
    /// </summary>
    /// <param name="input">The data pipe containing items to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<object> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("ConsoleSink started processing items...");

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            _processedCount++;

            Console.WriteLine();
            Console.WriteLine($"=== Console Sink Output #{_processedCount} ===");

            switch (item)
            {
                case SalesByCategory sales:
                    Console.WriteLine($"Sales by Category: {sales.Category}");
                    Console.WriteLine($"  Total Orders: {sales.TotalOrders}");
                    Console.WriteLine($"  Total Revenue: ${sales.TotalRevenue:F2}");
                    Console.WriteLine($"  Average Order Value: ${sales.AverageOrderValue:F2}");
                    Console.WriteLine($"  Window: {sales.WindowStart:O} to {sales.WindowEnd:O}");
                    break;

                case CustomerPurchaseBehavior customer:
                    Console.WriteLine($"Customer Behavior: {customer.CustomerName}");
                    Console.WriteLine($"  Customer ID: {customer.CustomerId}");
                    Console.WriteLine($"  Country: {customer.Country}");
                    Console.WriteLine($"  Total Orders: {customer.TotalOrders}");
                    Console.WriteLine($"  Total Spent: ${customer.TotalSpent:F2}");
                    Console.WriteLine($"  Average Order Value: ${customer.AverageOrderValue:F2}");
                    Console.WriteLine($"  First Order: {customer.FirstOrderDate:yyyy-MM-dd}");
                    Console.WriteLine($"  Last Order: {customer.LastOrderDate:yyyy-MM-dd}");
                    Console.WriteLine($"  Preferred Categories: {string.Join(", ", customer.PreferredCategories)}");
                    break;

                case Product product:
                    Console.WriteLine($"Product: {product.Name}");
                    Console.WriteLine($"  Product ID: {product.ProductId}");
                    Console.WriteLine($"  Category: {product.Category}");
                    Console.WriteLine($"  Price: ${product.Price:F2}");
                    Console.WriteLine($"  Stock Quantity: {product.StockQuantity}");
                    break;

                default:
                    Console.WriteLine($"Unknown item type: {item?.GetType().Name}");
                    Console.WriteLine($"  Value: {item}");
                    break;
            }

            Console.WriteLine($"=== End Output #{_processedCount} ===");
            Console.WriteLine();

            // Simulate some processing time
            await Task.Delay(10, cancellationToken);
        }

        Console.WriteLine($"ConsoleSink completed processing {_processedCount} items.");
    }
}
