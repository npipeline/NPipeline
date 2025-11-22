using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_13_KeyedJoinNode.Nodes;

/// <summary>
///     Transform node that enriches order-customer joins with product information.
///     This node demonstrates how to perform lookups to enrich data streams with additional information.
///     It maintains an in-memory product catalog for fast lookups.
/// </summary>
public class ProductLookupNode : TransformNode<OrderCustomerJoin, EnrichedOrder>
{
    private readonly ILogger<ProductLookupNode>? _logger;
    private readonly ConcurrentDictionary<string, Product> _productCatalog;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ProductLookupNode" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    public ProductLookupNode(ILogger<ProductLookupNode>? logger = null)
    {
        _logger = logger;
        _productCatalog = new ConcurrentDictionary<string, Product>();

        // Initialize with sample product data
        InitializeProductCatalog();
    }

    /// <inheritdoc />
    public override Task<EnrichedOrder> ExecuteAsync(OrderCustomerJoin item, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger?.LogDebug("ProductLookupNode: Processing OrderCustomerJoin for Order {OrderId}", item.Order.OrderId);

        var productCode = item.Order.ProductCode;

        if (_productCatalog.TryGetValue(productCode, out var product))
        {
            var enrichedOrder = new EnrichedOrder(item, product);

            _logger?.LogDebug("ProductLookupNode: Enriched Order {OrderId} with Product {ProductName}",
                item.Order.OrderId, product.ProductName);

            Console.WriteLine($"📦 Enriched Order {item.Order.OrderId} with Product: {product.ProductName} ({product.Category}) - {item.Order.TotalPrice:C}");

            return Task.FromResult(enrichedOrder);
        }

        _logger?.LogWarning("ProductLookupNode: Product {ProductCode} not found for Order {OrderId}",
            productCode, item.Order.OrderId);

        Console.WriteLine($"❌ Product {productCode} not found for Order {item.Order.OrderId}");

        // Create enriched order with unknown product
        var unknownProduct = new Product(
            productCode,
            "Unknown Product",
            "Unknown Category",
            0m
        );

        return Task.FromResult(new EnrichedOrder(item, unknownProduct));
    }

    /// <summary>
    ///     Initializes the product catalog with sample data.
    /// </summary>
    private void InitializeProductCatalog()
    {
        var products = new[]
        {
            new Product("ELEC-001", "Smartphone X1", "Electronics", 299.99m),
            new Product("ELEC-002", "Laptop Pro", "Electronics", 599.99m),
            new Product("CLOTH-001", "Cotton T-Shirt", "Clothing", 49.99m),
            new Product("CLOTH-002", "Denim Jeans", "Clothing", 79.99m),
            new Product("BOOK-001", "Programming Guide", "Books", 19.99m),
            new Product("BOOK-002", "Design Patterns", "Books", 29.99m),
            new Product("HOME-001", "Coffee Maker", "Home & Garden", 149.99m),
            new Product("HOME-002", "Blender", "Home & Garden", 199.99m),
        };

        foreach (var product in products)
        {
            _productCatalog.TryAdd(product.ProductCode, product);
        }

        _logger?.LogInformation("ProductLookupNode: Initialized product catalog with {Count} products", _productCatalog.Count);
    }
}
