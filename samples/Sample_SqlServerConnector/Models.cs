using System.Data;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Mapping;

namespace Sample_SqlServerConnector;

/// <summary>
///     Represents a customer entity with SQL Server-specific mapping attributes.
///     Demonstrates the use of SqlServerTable, SqlServerColumn, Column, and IgnoreColumn attributes.
/// </summary>
[SqlServerTable("Customers", Schema = "Sales")]
public sealed class Customer
{
    /// <summary>
    ///     Gets or sets the unique customer identifier.
    ///     Uses SqlServerColumn with PrimaryKey and Identity for auto-increment.
    /// </summary>
    [SqlServerColumn("CustomerID", PrimaryKey = true, Identity = true)]
    public int CustomerId { get; set; }

    /// <summary>
    ///     Gets or sets the customer's first name.
    ///     Uses SqlServerColumn with DbType and Size for precise column definition.
    /// </summary>
    [SqlServerColumn("FirstName", DbType = SqlDbType.NVarChar, Size = 100)]
    public string FirstName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's last name.
    ///     Uses the common Column attribute for simple mapping (recommended for basic mappings).
    /// </summary>
    [Column("LastName")]
    public string LastName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's email address.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("Email")]
    public string Email { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's phone number.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("PhoneNumber")]
    public string? PhoneNumber { get; set; }

    /// <summary>
    ///     Gets or sets the date when the customer registered.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("RegistrationDate", DbType = SqlDbType.Date)]
    public DateTime RegistrationDate { get; set; }

    /// <summary>
    ///     Gets or sets the customer's status (Active, Inactive, Suspended).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("Status")]
    public string Status { get; set; } = "Active";

    /// <summary>
    ///     Gets the customer's full name (computed property).
    ///     Marked with IgnoreColumn to exclude from database mapping.
    /// </summary>
    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}

/// <summary>
///     Represents an order entity with SQL Server-specific mapping attributes.
///     Demonstrates attribute-based mapping with various SQL Server features.
/// </summary>
[SqlServerTable("Orders", Schema = "Sales")]
public sealed class Order
{
    /// <summary>
    ///     Gets or sets the unique order identifier.
    ///     Uses SqlServerColumn with PrimaryKey and Identity.
    /// </summary>
    [SqlServerColumn("OrderID", PrimaryKey = true, Identity = true)]
    public int OrderId { get; set; }

    /// <summary>
    ///     Gets or sets the customer ID (foreign key).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("CustomerID")]
    public int CustomerId { get; set; }

    /// <summary>
    ///     Gets or sets the order date.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("OrderDate", DbType = SqlDbType.DateTime2)]
    public DateTime OrderDate { get; set; }

    /// <summary>
    ///     Gets or sets the total amount of the order.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("TotalAmount", DbType = SqlDbType.Decimal)]
    public decimal TotalAmount { get; set; }

    /// <summary>
    ///     Gets or sets the order status (Pending, Processing, Shipped, Delivered, Cancelled).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("Status")]
    public string Status { get; set; } = "Pending";

    /// <summary>
    ///     Gets or sets the shipping address.
    ///     Uses SqlServerColumn with DbType and Size for precise column definition.
    /// </summary>
    [SqlServerColumn("ShippingAddress", DbType = SqlDbType.NVarChar, Size = 500)]
    public string? ShippingAddress { get; set; }

    /// <summary>
    ///     Gets or sets the notes for the order.
    ///     Uses SqlServerColumn with DbType and Size for precise column definition.
    /// </summary>
    [SqlServerColumn("Notes", DbType = SqlDbType.NVarChar, Size = 1000)]
    public string? Notes { get; set; }

    /// <summary>
    ///     Gets whether the order is high value (computed property).
    ///     Marked with IgnoreColumn to exclude from database mapping.
    /// </summary>
    [IgnoreColumn]
    public bool IsHighValue => TotalAmount > 1000m;
}

/// <summary>
///     Represents an enriched customer with additional computed properties.
///     Demonstrates transformation and computed property handling.
/// </summary>
[SqlServerTable("EnrichedCustomers", Schema = "Analytics")]
public sealed class EnrichedCustomer
{
    /// <summary>
    ///     Gets or sets the customer ID.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("CustomerID")]
    public int CustomerId { get; set; }

    /// <summary>
    ///     Gets or sets the customer's full name (stored in this table).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("FullName")]
    public string FullName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's email address (uppercase).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("Email")]
    public string Email { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's phone number.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("PhoneNumber")]
    public string? PhoneNumber { get; set; }

    /// <summary>
    ///     Gets or sets the customer registration date.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("RegistrationDate")]
    public DateTime RegistrationDate { get; set; }

    /// <summary>
    ///     Gets or sets the customer's status.
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("Status")]
    public string Status { get; set; } = "Active";

    /// <summary>
    ///     Gets or sets the total order count for the customer.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("TotalOrders", DbType = SqlDbType.Int)]
    public int TotalOrders { get; set; }

    /// <summary>
    ///     Gets or sets the total amount spent by the customer.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("TotalSpent", DbType = SqlDbType.Decimal)]
    public decimal TotalSpent { get; set; }

    /// <summary>
    ///     Gets or sets the average order value.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("AverageOrderValue", DbType = SqlDbType.Decimal)]
    public decimal AverageOrderValue { get; set; }

    /// <summary>
    ///     Gets or sets the customer tier (Bronze, Silver, Gold, Platinum).
    ///     Uses the common Column attribute for simple mapping.
    /// </summary>
    [Column("CustomerTier")]
    public string CustomerTier { get; set; } = "Bronze";

    /// <summary>
    ///     Gets or sets the last order date.
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("LastOrderDate", DbType = SqlDbType.DateTime2)]
    public DateTime? LastOrderDate { get; set; }

    /// <summary>
    ///     Gets or sets the enrichment date (when this record was created).
    ///     Uses SqlServerColumn with DbType for precise type mapping.
    /// </summary>
    [SqlServerColumn("EnrichmentDate", DbType = SqlDbType.DateTime2)]
    public DateTime EnrichmentDate { get; set; }

    /// <summary>
    ///     Gets whether the customer is premium (computed property).
    ///     Marked with IgnoreColumn to exclude from database mapping.
    /// </summary>
    [IgnoreColumn]
    public bool IsPremium => CustomerTier is "Gold" or "Platinum";

    /// <summary>
    ///     Gets whether the customer is inactive (computed property).
    ///     Marked with IgnoreColumn to exclude from database mapping.
    /// </summary>
    [IgnoreColumn]
    public bool IsInactive => Status == "Inactive";
}

/// <summary>
///     Represents a simple product entity for convention-based mapping demonstration.
///     No attributes are used - mapping relies on convention (PascalCase property names match PascalCase column names).
/// </summary>
public sealed class Product
{
    /// <summary>
    ///     Gets or sets the product ID.
    ///     Convention-based mapping: Property "ProductId" maps to column "ProductId".
    /// </summary>
    public int ProductId { get; set; }

    /// <summary>
    ///     Gets or sets the product name.
    ///     Convention-based mapping: Property "ProductName" maps to column "ProductName".
    /// </summary>
    public string ProductName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the product category.
    ///     Convention-based mapping: Property "Category" maps to column "Category".
    /// </summary>
    public string Category { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the product price.
    ///     Convention-based mapping: Property "Price" maps to column "Price".
    /// </summary>
    public decimal Price { get; set; }

    /// <summary>
    ///     Gets or sets the stock quantity.
    ///     Convention-based mapping: Property "StockQuantity" maps to column "StockQuantity".
    /// </summary>
    public int StockQuantity { get; set; }

    /// <summary>
    ///     Gets whether the product is in stock (computed property).
    ///     Marked with IgnoreColumn to exclude from database mapping.
    /// </summary>
    [IgnoreColumn]
    public bool InStock => StockQuantity > 0;
}
