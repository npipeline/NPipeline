using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Source node that generates product data for the complex data transformations pipeline.
///     This node creates realistic e-commerce product data with various categories and prices.
/// </summary>
public class ProductSource : SourceNode<Product>
{
    /// <summary>
    ///     Generates a collection of products with realistic e-commerce data.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated products.</returns>
    public override IDataPipe<Product> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating product data...");

        var random = new Random(42); // Fixed seed for reproducible results
        var products = new List<Product>();

        var productNames = new[]
        {
            "Laptop Pro", "Wireless Mouse", "USB-C Hub", "Mechanical Keyboard", "4K Monitor",
            "Smartphone X", "Wireless Headphones", "Tablet Pro", "Smart Watch", "Bluetooth Speaker",
            "External SSD", "Webcam HD", "Gaming Chair", "Desk Lamp", "Power Bank",
            "HDMI Cable", "Network Switch", "Router WiFi", "Graphics Card", "RAM Module",
        };

        var categories = new[] { "Electronics", "Computers", "Accessories", "Mobile", "Audio", "Storage", "Gaming" };

        // Generate 20 products
        for (var i = 1; i <= 20; i++)
        {
            var name = productNames[i - 1];
            var category = categories[random.Next(categories.Length)];
            var price = Math.Round(random.NextDecimal(10, 1000), 2);
            var stockQuantity = random.Next(0, 100);

            products.Add(new Product(i, name, category, price, stockQuantity));
        }

        Console.WriteLine($"Generated {products.Count} products");
        return new ListDataPipe<Product>(products, "ProductSource");
    }
}
