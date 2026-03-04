using NPipeline.Connectors.MongoDB.Attributes;

namespace Sample_MongoDbConnector;

/// <summary>
///     Represents an order in the shop database.
/// </summary>
/// <remarks>
///     The <see cref="MongoCollectionAttribute" /> specifies the target collection name.
///     The <see cref="MongoFieldAttribute" /> maps properties to MongoDB field names.
/// </remarks>
[MongoCollection("orders")]
public sealed record Order
{
    /// <summary>Gets or sets the order identifier (maps to MongoDB _id field).</summary>
    [MongoField("_id")]
    public string Id { get; set; } = string.Empty;

    /// <summary>Gets or sets the customer name.</summary>
    [MongoField("customer")]
    public string Customer { get; set; } = string.Empty;

    /// <summary>Gets or sets the order amount.</summary>
    [MongoField("amount")]
    public decimal Amount { get; set; }

    /// <summary>Gets or sets the order status.</summary>
    [MongoField("status")]
    public string Status { get; set; } = string.Empty;

    /// <summary>Gets or sets the creation timestamp.</summary>
    [MongoField("createdAt")]
    public DateTime CreatedAt { get; set; }
}

/// <summary>
///     Represents a processed order with calculated fields.
/// </summary>
/// <remarks>
///     This is the output of the ETL pipeline transformation.
///     The <see cref="MongoCollectionAttribute" /> specifies the target collection for processed orders.
/// </remarks>
[MongoCollection("processed_orders")]
public sealed record ProcessedOrder
{
    /// <summary>Gets or sets the order identifier.</summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>Gets or sets the customer name.</summary>
    public string Customer { get; set; } = string.Empty;

    /// <summary>Gets or sets the original order amount.</summary>
    public decimal Amount { get; set; }

    /// <summary>Gets or sets the calculated tax amount (10% of amount).</summary>
    public decimal Tax { get; set; }

    /// <summary>Gets or sets the total amount (amount + tax).</summary>
    public decimal Total { get; set; }

    /// <summary>Gets or sets the processing timestamp.</summary>
    public string ProcessedAt { get; set; } = string.Empty;
}
