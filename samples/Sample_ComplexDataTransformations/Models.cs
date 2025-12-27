namespace Sample_ComplexDataTransformations;

/// <summary>
///     Represents an order in the e-commerce system
/// </summary>
public record Order(
    int OrderId,
    int CustomerId,
    DateTime OrderDate,
    decimal TotalAmount,
    string Status
);

/// <summary>
///     Represents customer information
/// </summary>
public record Customer(
    int CustomerId,
    string Name,
    string Email,
    string Country,
    DateTime RegistrationDate
);

/// <summary>
///     Represents product information
/// </summary>
public record Product(
    int ProductId,
    string Name,
    string Category,
    decimal Price,
    int StockQuantity
);

/// <summary>
///     Represents an order line item
/// </summary>
public record OrderItem(
    int OrderId,
    int ProductId,
    int Quantity,
    decimal UnitPrice
);

/// <summary>
///     Represents enriched order data after lookup operations
/// </summary>
public record EnrichedOrder(
    Order Order,
    Customer Customer,
    List<OrderItem> Items,
    decimal TotalValue,
    string CustomerCountry,
    DateTime ProcessingTimestamp
);

/// <summary>
///     Represents joined order and customer data
/// </summary>
public record OrderCustomerJoin(
    Order Order,
    Customer Customer
);

/// <summary>
///     Represents sales aggregation by category
/// </summary>
public record SalesByCategory(
    string Category,
    int TotalOrders,
    decimal TotalRevenue,
    decimal AverageOrderValue,
    DateTime WindowStart,
    DateTime WindowEnd
);

/// <summary>
///     Represents customer purchase behavior aggregation
/// </summary>
public record CustomerPurchaseBehavior(
    int CustomerId,
    string CustomerName,
    string Country,
    int TotalOrders,
    decimal TotalSpent,
    decimal AverageOrderValue,
    DateTime FirstOrderDate,
    DateTime LastOrderDate,
    List<string> PreferredCategories
);

/// <summary>
///     Represents lineage tracking information for data transformations
/// </summary>
public record TransformationLineage(
    Guid LineageId,
    string SourceNode,
    string TargetNode,
    DateTime TransformationTime,
    string Operation,
    Dictionary<string, object> Metadata
)
{
    public static TransformationLineage Create<T>(string sourceNode, string targetNode, string operation, T data)
    {
        return new TransformationLineage(
            Guid.NewGuid(),
            sourceNode,
            targetNode,
            DateTime.UtcNow,
            operation,
            new Dictionary<string, object>
            {
                ["DataType"] = typeof(T).Name,
                ["Data"] = data?.ToString() ?? "null",
            }
        );
    }
}

/// <summary>
///     Helper class for creating LineageTrackedItem instances
/// </summary>
public static class LineageTrackedItemFactory
{
    public static LineageTrackedItem<T> Create<T>(T data, string sourceNode)
    {
        var lineage = new List<TransformationLineage>
        {
            TransformationLineage.Create(sourceNode, sourceNode, "Source", data),
        };

        return new LineageTrackedItem<T>(data, lineage);
    }
}

/// <summary>
///     Represents a data item with lineage tracking
/// </summary>
public record LineageTrackedItem<T>(
    T Data,
    List<TransformationLineage> Lineage
)
{
    public LineageTrackedItem<T> AddTransformation(string sourceNode, string targetNode, string operation)
    {
        var newLineage = new List<TransformationLineage>(Lineage)
        {
            TransformationLineage.Create(sourceNode, targetNode, operation, Data),
        };

        return new LineageTrackedItem<T>(Data, newLineage);
    }
}

/// <summary>
///     Represents product lookup result for enrichment
/// </summary>
public record ProductLookupResult(
    Product? Product,
    bool Found,
    string LookupMessage
);

/// <summary>
///     Represents time-windowed aggregation result
/// </summary>
public record TimeWindowedResult<T>(
    T Result,
    DateTime WindowStart,
    DateTime WindowEnd,
    int ItemCount
);
