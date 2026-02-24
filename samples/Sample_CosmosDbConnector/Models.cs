using NPipeline.Connectors.Azure.CosmosDb.Mapping;

namespace Sample_CosmosDbConnector;

/// <summary>
///     Represents a product in the inventory catalog.
/// </summary>
/// <remarks>
///     <para>
///         The <c>id</c> property maps to the Cosmos DB document identifier by convention
///         (case-insensitive match on "id").
///     </para>
///     <para>
///         The <see cref="CosmosPartitionKeyAttribute" /> on <see cref="Category" /> tells
///         the sink node which property holds the partition key value so it can be extracted
///         automatically without a manual <c>partitionKeySelector</c> delegate.
///     </para>
/// </remarks>
public sealed record Product
{
    /// <summary>Gets or sets the document identifier (maps to Cosmos DB <c>id</c> field).</summary>
    public string id { get; set; } = string.Empty;

    /// <summary>Gets or sets the product category — also serves as the partition key.</summary>
    [CosmosPartitionKey]
    public string Category { get; set; } = string.Empty;

    /// <summary>Gets or sets the human-readable product name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the retail price in USD.</summary>
    public decimal Price { get; set; }

    /// <summary>Gets or sets the current stock level.</summary>
    public int Stock { get; set; }

    /// <summary>Gets or sets the timestamp of the most recent stock update.</summary>
    public DateTime LastUpdated { get; set; }
}

/// <summary>
///     A lightweight read-only projection returned by a SQL query over the products container.
/// </summary>
/// <remarks>
///     Demonstrates that source queries can return arbitrary projections — the property
///     names just need to match the aliases used in the SELECT clause.
/// </remarks>
public sealed record ProductSummary
{
    /// <summary>Gets or sets the document identifier.</summary>
    public string id { get; set; } = string.Empty;

    /// <summary>Gets or sets the product name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the product category.</summary>
    public string Category { get; set; } = string.Empty;

    /// <summary>Gets or sets the retail price in USD.</summary>
    public decimal Price { get; set; }

    /// <summary>Gets or sets the stock level at the time the query ran.</summary>
    public int Stock { get; set; }
}
