namespace NPipeline.Connectors.Azure.CosmosDb.Mapping;

/// <summary>
///     Marks a property as the partition key for Cosmos DB documents.
///     Used by the sink node to automatically extract partition keys for write operations.
/// </summary>
/// <remarks>
///     <para>
///         Only one property per class should be marked with this attribute.
///         The partition key is required for all write operations to Cosmos DB containers.
///     </para>
///     <para>
///         If no property is marked, the sink node will look for properties named
///         "PartitionKey", "partitionKey", or "partitionKeyPath" by convention.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// public record Order
/// {
///     public string Id { get; init; }
///     
///     [CosmosPartitionKey]
///     public string CustomerId { get; init; }
///     
///     public decimal Total { get; init; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property)]
public class CosmosPartitionKeyAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosPartitionKeyAttribute" /> class.
    /// </summary>
    public CosmosPartitionKeyAttribute()
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosPartitionKeyAttribute" /> class
    ///     with a specified partition key path.
    /// </summary>
    /// <param name="path">The Cosmos DB partition key path.</param>
    public CosmosPartitionKeyAttribute(string? path)
    {
        Path = path;
    }

    /// <summary>
    ///     Gets or sets the Cosmos DB partition key path.
    ///     If not specified, the property name is used.
    /// </summary>
    /// <remarks>
    ///     The path should be in Cosmos DB format, e.g., "/customerId".
    ///     This is primarily used for container creation and documentation.
    /// </remarks>
    public string? Path { get; set; }

    /// <summary>
    ///     Gets or sets whether this property is the partition key.
    ///     Always true when the attribute is present.
    /// </summary>
    public bool IsPartitionKey { get; set; } = true;
}
