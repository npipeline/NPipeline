using System.Data;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Mapping;

namespace Sample_SnowflakeConnector;

/// <summary>
///     Represents a customer record in the CUSTOMERS table.
///     Uses Snowflake-specific attributes with uppercase column names.
/// </summary>
[SnowflakeTable("CUSTOMERS", Schema = "PUBLIC")]
public sealed class Customer
{
    [SnowflakeColumn("ID", PrimaryKey = true)]
    public int Id { get; set; }

    [SnowflakeColumn("FIRST_NAME")]
    public string FirstName { get; set; } = string.Empty;

    [SnowflakeColumn("LAST_NAME")]
    public string LastName { get; set; } = string.Empty;

    [SnowflakeColumn("EMAIL")]
    public string Email { get; set; } = string.Empty;

    [SnowflakeColumn("PHONE_NUMBER")]
    public string? PhoneNumber { get; set; }

    [SnowflakeColumn("CREATED_AT", DbType = DbType.DateTime2, NativeTypeName = "TIMESTAMP_NTZ")]
    public DateTime CreatedAt { get; set; }

    [SnowflakeColumn("STATUS")]
    public string Status { get; set; } = "Active";

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}

/// <summary>
///     Represents an order record in the ORDERS table.
///     Uses Snowflake-specific attributes with uppercase column names.
/// </summary>
[SnowflakeTable("ORDERS", Schema = "PUBLIC")]
public sealed class Order
{
    [SnowflakeColumn("ORDER_ID", PrimaryKey = true)]
    public int OrderId { get; set; }

    [SnowflakeColumn("CUSTOMER_ID")]
    public int CustomerId { get; set; }

    [SnowflakeColumn("ORDER_DATE", DbType = DbType.DateTime2, NativeTypeName = "TIMESTAMP_NTZ")]
    public DateTime OrderDate { get; set; }

    [SnowflakeColumn("AMOUNT", DbType = DbType.Decimal, NativeTypeName = "NUMBER(18,2)")]
    public decimal Amount { get; set; }

    [SnowflakeColumn("STATUS")]
    public string Status { get; set; } = "Pending";

    [SnowflakeColumn("SHIPPING_ADDRESS")]
    public string? ShippingAddress { get; set; }

    [SnowflakeColumn("NOTES")]
    public string? Notes { get; set; }

    [IgnoreColumn]
    public bool IsHighValue => Amount > 1000m;
}

/// <summary>
///     Represents an enriched customer record with order summary data.
///     Written to the ENRICHED_CUSTOMERS table.
/// </summary>
[SnowflakeTable("ENRICHED_CUSTOMERS", Schema = "PUBLIC")]
public sealed class EnrichedCustomer
{
    [SnowflakeColumn("CUSTOMER_ID")]
    public int CustomerId { get; set; }

    [SnowflakeColumn("FULL_NAME")]
    public string FullName { get; set; } = string.Empty;

    [SnowflakeColumn("EMAIL")]
    public string Email { get; set; } = string.Empty;

    [SnowflakeColumn("PHONE_NUMBER")]
    public string? PhoneNumber { get; set; }

    [SnowflakeColumn("CREATED_AT", NativeTypeName = "TIMESTAMP_NTZ")]
    public DateTime CreatedAt { get; set; }

    [SnowflakeColumn("STATUS")]
    public string Status { get; set; } = "Active";

    [SnowflakeColumn("TOTAL_ORDERS", DbType = DbType.Int32)]
    public int TotalOrders { get; set; }

    [SnowflakeColumn("TOTAL_SPENT", DbType = DbType.Decimal, NativeTypeName = "NUMBER(18,2)")]
    public decimal TotalSpent { get; set; }

    [SnowflakeColumn("AVERAGE_ORDER_VALUE", DbType = DbType.Decimal, NativeTypeName = "NUMBER(18,2)")]
    public decimal AverageOrderValue { get; set; }

    [SnowflakeColumn("CUSTOMER_TIER")]
    public string CustomerTier { get; set; } = "Bronze";

    [SnowflakeColumn("LAST_ORDER_DATE", NativeTypeName = "TIMESTAMP_NTZ")]
    public DateTime? LastOrderDate { get; set; }

    [SnowflakeColumn("ENRICHMENT_DATE", NativeTypeName = "TIMESTAMP_NTZ")]
    public DateTime EnrichmentDate { get; set; }

    [IgnoreColumn]
    public bool IsPremium => CustomerTier is "Gold" or "Platinum";

    [IgnoreColumn]
    public bool IsInactive => Status == "Inactive";
}

/// <summary>
///     Convention-based mapping model (no attributes).
///     Snowflake uppercase convention is applied automatically.
/// </summary>
public sealed class OrderSummary
{
    public int CustomerId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public int TotalOrders { get; set; }
    public decimal TotalAmount { get; set; }
}
