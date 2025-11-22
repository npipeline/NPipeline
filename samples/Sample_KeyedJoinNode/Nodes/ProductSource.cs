using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_KeyedJoinNode.Nodes;

/// <summary>
///     Source node that generates sample product data for demonstrating keyed join functionality.
///     This node creates a stream of products with different categories and prices.
/// </summary>
public class ProductSource : SourceNode<Product>
{
    private readonly TimeSpan _delayBetweenProducts;
    private readonly ILogger<ProductSource>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ProductSource" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="delayBetweenProducts">Delay between generating products to simulate real-time data.</param>
    public ProductSource(ILogger<ProductSource>? logger = null, TimeSpan? delayBetweenProducts = null)
    {
        _logger = logger;
        _delayBetweenProducts = delayBetweenProducts ?? TimeSpan.FromMilliseconds(100);
    }

    /// <inheritdoc />
    public override IDataPipe<Product> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger?.LogInformation("ProductSource: Starting to generate products");

        // Generate sample products with different categories
        var products = new List<Product>
        {
            new("ELEC-001", "Smartphone X1", "Electronics", 299.99m),
            new("ELEC-002", "Laptop Pro", "Electronics", 599.99m),
            new("CLOTH-001", "Cotton T-Shirt", "Clothing", 49.99m),
            new("CLOTH-002", "Denim Jeans", "Clothing", 79.99m),
            new("BOOK-001", "Programming Guide", "Books", 19.99m),
            new("BOOK-002", "Design Patterns", "Books", 29.99m),
            new("HOME-001", "Coffee Maker", "Home & Garden", 149.99m),
            new("HOME-002", "Blender", "Home & Garden", 199.99m),
        };

        foreach (var product in products)
        {
            _logger?.LogDebug("ProductSource: Generated Product {ProductCode} - {ProductName}, Category: {Category}, Price: {Price:C}",
                product.ProductCode, product.ProductName, product.Category, product.Price);
        }

        _logger?.LogInformation("ProductSource: Finished generating {Count} products", products.Count);

        return new ListDataPipe<Product>(products, "ProductSource");
    }
}
