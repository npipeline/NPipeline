using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_10_ComplexDataTransformations.Nodes;

/// <summary>
///     Lookup node that enriches order-customer joins with product information.
///     This node demonstrates external data lookups in NPipeline using the LookupNode base class.
///     It simulates product lookups to enrich order data with product details.
/// </summary>
public class ProductLookupNode : LookupNode<OrderCustomerJoin, int, Product, EnrichedOrder>
{
    private readonly Dictionary<int, Product> _productCache = new();

    /// <summary>
    ///     Initializes a new instance of the ProductLookupNode.
    /// </summary>
    public ProductLookupNode()
    {
        // Initialize with some sample products for demonstration
        InitializeProductCache();
    }

    /// <summary>
    ///     Extracts the lookup key from the input item.
    ///     For this example, we'll use a simulated ProductId from the OrderId.
    /// </summary>
    /// <param name="input">The OrderCustomerJoin input item.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The ProductId to use for the lookup.</returns>
    protected override int ExtractKey(OrderCustomerJoin input, PipelineContext context)
    {
        // In a real scenario, this would extract ProductId from OrderItems
        // For this example, we simulate it by using OrderId % 20 + 1 to get a valid ProductId
        var productId = input.Order?.OrderId % 20 + 1 ?? 1;
        Console.WriteLine($"Extracting ProductId {productId} from Order {input.Order?.OrderId}");
        return productId;
    }

    /// <summary>
    ///     Performs the asynchronous lookup operation to retrieve product information.
    /// </summary>
    /// <param name="key">The ProductId to look up.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous lookup operation.</returns>
    protected override async Task<Product?> LookupAsync(int key, PipelineContext context, CancellationToken cancellationToken)
    {
        // Simulate external lookup delay
        await Task.Delay(10, cancellationToken);

        Console.WriteLine($"Looking up ProductId {key}...");

        // Simulate cache miss scenario
        if (_productCache.TryGetValue(key, out var product))
        {
            Console.WriteLine($"Found Product: {product.Name} (Category: {product.Category}, Price: ${product.Price})");
            return product;
        }

        Console.WriteLine($"Product {key} not found");
        return null;
    }

    /// <summary>
    ///     Creates the final enriched output by combining order-customer data with product information.
    /// </summary>
    /// <param name="input">The original OrderCustomerJoin input item.</param>
    /// <param name="lookupValue">The product retrieved from the lookup, or null if not found.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The enriched order with product information.</returns>
    protected override EnrichedOrder CreateOutput(OrderCustomerJoin input, Product? lookupValue, PipelineContext context)
    {
        var order = input.Order;
        var customer = input.Customer;

        // Create order items list (simulated)
        var items = new List<OrderItem>();

        if (lookupValue != null && order != null)
            items.Add(new OrderItem(order.OrderId, lookupValue.ProductId, 1, lookupValue.Price));

        // Calculate total value
        var totalValue = items.Sum(item => item.Quantity * item.UnitPrice);

        var enrichedOrder = new EnrichedOrder(
            order!,
            customer,
            items,
            totalValue,
            customer?.Country ?? "Unknown",
            DateTime.UtcNow
        );

        Console.WriteLine($"Created enriched order for Customer {customer?.Name} with {items.Count} items, Total: ${totalValue}");
        return enrichedOrder;
    }

    /// <summary>
    ///     Initializes the product cache with sample data.
    ///     In a real implementation, this would load from a database or external service.
    /// </summary>
    private void InitializeProductCache()
    {
        var random = new Random(42);
        var categories = new[] { "Electronics", "Computers", "Accessories", "Mobile", "Audio", "Storage", "Gaming" };

        for (var i = 1; i <= 20; i++)
        {
            var category = categories[random.Next(categories.Length)];
            var price = Math.Round(random.NextDecimal(10, 1000), 2);
            var stock = random.Next(0, 100);

            _productCache[i] = new Product(
                i,
                $"Product {i}",
                category,
                price,
                stock
            );
        }

        Console.WriteLine($"Initialized product cache with {_productCache.Count} products");
    }
}
