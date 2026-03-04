using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Mapping;

namespace Sample_MySQLConnector;

/// <summary>
///     Represents a product entity with MySQL-specific mapping attributes.
///     Demonstrates the use of MySqlTable, MySqlColumn, Column, and IgnoreColumn attributes.
/// </summary>
[MySqlTable("products")]
public sealed class Product
{
    /// <summary>
    ///     Gets or sets the unique product identifier.
    ///     Uses MySqlColumn with AutoIncrement for auto-increment primary key.
    /// </summary>
    [MySqlColumn("product_id", AutoIncrement = true)]
    public int ProductId { get; set; }

    /// <summary>
    ///     Gets or sets the product name.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("product_name")]
    public string ProductName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the product category.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("category")]
    public string Category { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the unit price.
    ///     Uses MySqlColumn for explicit column name.
    /// </summary>
    [MySqlColumn("unit_price")]
    public decimal UnitPrice { get; set; }

    /// <summary>
    ///     Gets or sets the current stock quantity.
    ///     Uses MySqlColumn for explicit column name.
    /// </summary>
    [MySqlColumn("stock_quantity")]
    public int StockQuantity { get; set; }

    /// <summary>
    ///     Gets or sets whether the product is active.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("is_active")]
    public bool IsActive { get; set; } = true;

    /// <summary>
    ///     Gets or sets the creation date.
    ///     Uses MySqlColumn for explicit column name.
    /// </summary>
    [MySqlColumn("created_at")]
    public DateTime CreatedAt { get; set; }

    /// <summary>
    ///     Gets whether the product is in stock (computed property).
    ///     Marked with IgnoreColumn to exclude from database mapping.
    /// </summary>
    [IgnoreColumn]
    public bool InStock => StockQuantity > 0;
}

/// <summary>
///     Represents an order event entity with MySQL-specific mapping attributes.
///     Demonstrates upsert patterns using ON DUPLICATE KEY UPDATE.
/// </summary>
[MySqlTable("order_events")]
public sealed class OrderEvent
{
    /// <summary>
    ///     Gets or sets the unique event identifier (used as upsert key).
    /// </summary>
    [MySqlColumn("event_id")]
    public string EventId { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the order ID.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("order_id")]
    public int OrderId { get; set; }

    /// <summary>
    ///     Gets or sets the event type (created, updated, shipped, delivered).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("event_type")]
    public string EventType { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the event payload as JSON.
    ///     Uses MySqlColumn for explicit column name.
    /// </summary>
    [MySqlColumn("event_payload")]
    public string? EventPayload { get; set; }

    /// <summary>
    ///     Gets or sets the event timestamp.
    ///     Uses MySqlColumn for explicit column name.
    /// </summary>
    [MySqlColumn("event_timestamp")]
    public DateTime EventTimestamp { get; set; }

    /// <summary>
    ///     Gets or sets the processing status (pending, processed, failed).
    /// </summary>
    [Column("status")]
    public string Status { get; set; } = "pending";
}

/// <summary>
///     Represents an enriched product for analytics write-through.
///     Demonstrates convention-based mapping — no attributes needed.
/// </summary>
public sealed class ProductSummary
{
    /// <summary>Gets or sets the product ID.</summary>
    public int ProductId { get; set; }

    /// <summary>Gets or sets the product name.</summary>
    public string ProductName { get; set; } = string.Empty;

    /// <summary>Gets or sets the category name.</summary>
    public string Category { get; set; } = string.Empty;

    /// <summary>Gets or sets the total revenue from this product.</summary>
    public decimal TotalRevenue { get; set; }

    /// <summary>Gets or sets the units sold.</summary>
    public int UnitsSold { get; set; }

    /// <summary>Gets or sets the last updated timestamp.</summary>
    public DateTime LastUpdatedAt { get; set; }

    /// <summary>Gets whether revenue exceeds a threshold (not stored).</summary>
    [IgnoreColumn]
    public bool IsTopSeller => TotalRevenue > 10_000m;
}
