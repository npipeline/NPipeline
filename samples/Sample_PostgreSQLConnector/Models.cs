using NPipeline.Connectors.PostgreSQL.Mapping;

namespace Sample_PostgreSQLConnector;

/// <summary>
///     Customer model representing a customer record in the database.
///     Demonstrates convention-based mapping with snake_case column names.
/// </summary>
[PostgresTable("customers")]
public class Customer
{
    /// <summary>Gets or sets the customer identifier.</summary>
    [PostgresColumn("customer_id", PrimaryKey = true)]
    public int CustomerId { get; set; }

    /// <summary>Gets or sets the customer's first name.</summary>
    [PostgresColumn("first_name")]
    public string FirstName { get; set; } = string.Empty;

    /// <summary>Gets or sets the customer's last name.</summary>
    [PostgresColumn("last_name")]
    public string LastName { get; set; } = string.Empty;

    /// <summary>Gets or sets the customer's email address.</summary>
    [PostgresColumn("email")]
    public string Email { get; set; } = string.Empty;

    /// <summary>Gets or sets the customer's phone number.</summary>
    [PostgresColumn("phone")]
    public string? Phone { get; set; }

    /// <summary>Gets or sets the customer's address.</summary>
    [PostgresColumn("address")]
    public string? Address { get; set; }

    /// <summary>Gets or sets the customer's city.</summary>
    [PostgresColumn("city")]
    public string? City { get; set; }

    /// <summary>Gets or sets the customer's state/province.</summary>
    [PostgresColumn("state")]
    public string? State { get; set; }

    /// <summary>Gets or sets the customer's postal code.</summary>
    [PostgresColumn("postal_code")]
    public string? PostalCode { get; set; }

    /// <summary>Gets or sets the customer's country.</summary>
    [PostgresColumn("country")]
    public string? Country { get; set; }

    /// <summary>Gets or sets the customer registration date.</summary>
    [PostgresColumn("registration_date")]
    public DateTime RegistrationDate { get; set; }

    /// <summary>Gets or sets the customer status (active, inactive, suspended).</summary>
    [PostgresColumn("status")]
    public string Status { get; set; } = "active";

    /// <summary>Gets the customer's full name.</summary>
    public string FullName => $"{FirstName} {LastName}";
}

/// <summary>
///     Product model representing a product in the catalog.
///     Demonstrates attribute-based mapping with custom column names.
/// </summary>
[PostgresTable("products")]
public class Product
{
    /// <summary>Gets or sets the product identifier.</summary>
    [PostgresColumn("product_id", PrimaryKey = true)]
    public int ProductId { get; set; }

    /// <summary>Gets or sets the product name.</summary>
    [PostgresColumn("product_name")]
    public string ProductName { get; set; } = string.Empty;

    /// <summary>Gets or sets the product SKU.</summary>
    [PostgresColumn("sku")]
    public string Sku { get; set; } = string.Empty;

    /// <summary>Gets or sets the product description.</summary>
    [PostgresColumn("description")]
    public string? Description { get; set; }

    /// <summary>Gets or sets the product category.</summary>
    [PostgresColumn("category")]
    public string Category { get; set; } = string.Empty;

    /// <summary>Gets or sets the product price.</summary>
    [PostgresColumn("price")]
    public decimal Price { get; set; }

    /// <summary>Gets or sets the product cost.</summary>
    [PostgresColumn("cost")]
    public decimal Cost { get; set; }

    /// <summary>Gets or sets the stock quantity.</summary>
    [PostgresColumn("stock_quantity")]
    public int StockQuantity { get; set; }

    /// <summary>Gets or sets the reorder level.</summary>
    [PostgresColumn("reorder_level")]
    public int ReorderLevel { get; set; }

    /// <summary>Gets or sets whether the product is active.</summary>
    [PostgresColumn("is_active")]
    public bool IsActive { get; set; } = true;

    /// <summary>Gets or sets the product creation date.</summary>
    [PostgresColumn("created_at")]
    public DateTime CreatedAt { get; set; }

    /// <summary>Gets or sets the last updated date.</summary>
    [PostgresColumn("updated_at")]
    public DateTime UpdatedAt { get; set; }

    /// <summary>Gets the profit margin.</summary>
    public decimal ProfitMargin => Price > 0
        ? (Price - Cost) / Price * 100
        : 0;
}

/// <summary>
///     Order model representing a customer order.
///     Demonstrates relationship mapping with foreign keys.
/// </summary>
[PostgresTable("orders")]
public class Order
{
    /// <summary>Gets or sets the order identifier.</summary>
    [PostgresColumn("order_id", PrimaryKey = true)]
    public int OrderId { get; set; }

    /// <summary>Gets or sets the customer identifier (foreign key).</summary>
    [PostgresColumn("customer_id")]
    public int CustomerId { get; set; }

    /// <summary>Gets or sets the order date.</summary>
    [PostgresColumn("order_date")]
    public DateTime OrderDate { get; set; }

    /// <summary>Gets or sets the order status.</summary>
    [PostgresColumn("status")]
    public string Status { get; set; } = "pending";

    /// <summary>Gets or sets the subtotal amount.</summary>
    [PostgresColumn("subtotal")]
    public decimal Subtotal { get; set; }

    /// <summary>Gets or sets the tax amount.</summary>
    [PostgresColumn("tax_amount")]
    public decimal TaxAmount { get; set; }

    /// <summary>Gets or sets the shipping amount.</summary>
    [PostgresColumn("shipping_amount")]
    public decimal ShippingAmount { get; set; }

    /// <summary>Gets or sets the discount amount.</summary>
    [PostgresColumn("discount_amount")]
    public decimal DiscountAmount { get; set; }

    /// <summary>Gets or sets the total amount.</summary>
    [PostgresColumn("total_amount")]
    public decimal TotalAmount { get; set; }

    /// <summary>Gets or sets the shipping address.</summary>
    [PostgresColumn("shipping_address")]
    public string? ShippingAddress { get; set; }

    /// <summary>Gets or sets the shipping city.</summary>
    [PostgresColumn("shipping_city")]
    public string? ShippingCity { get; set; }

    /// <summary>Gets or sets the shipping state.</summary>
    [PostgresColumn("shipping_state")]
    public string? ShippingState { get; set; }

    /// <summary>Gets or sets the shipping postal code.</summary>
    [PostgresColumn("shipping_postal_code")]
    public string? ShippingPostalCode { get; set; }

    /// <summary>Gets or sets the shipping country.</summary>
    [PostgresColumn("shipping_country")]
    public string? ShippingCountry { get; set; }

    /// <summary>Gets or sets the order notes.</summary>
    [PostgresColumn("notes")]
    public string? Notes { get; set; }

    /// <summary>Gets or sets the creation timestamp.</summary>
    [PostgresColumn("created_at")]
    public DateTime CreatedAt { get; set; }

    /// <summary>Gets or sets the last updated timestamp.</summary>
    [PostgresColumn("updated_at")]
    public DateTime UpdatedAt { get; set; }
}

/// <summary>
///     OrderItem model representing items within an order.
///     Demonstrates many-to-one relationship with orders and products.
/// </summary>
[PostgresTable("order_items")]
public class OrderItem
{
    /// <summary>Gets or sets the order item identifier.</summary>
    [PostgresColumn("order_item_id", PrimaryKey = true)]
    public int OrderItemId { get; set; }

    /// <summary>Gets or sets the order identifier (foreign key).</summary>
    [PostgresColumn("order_id")]
    public int OrderId { get; set; }

    /// <summary>Gets or sets the product identifier (foreign key).</summary>
    [PostgresColumn("product_id")]
    public int ProductId { get; set; }

    /// <summary>Gets or sets the quantity ordered.</summary>
    [PostgresColumn("quantity")]
    public int Quantity { get; set; }

    /// <summary>Gets or sets the unit price at time of order.</summary>
    [PostgresColumn("unit_price")]
    public decimal UnitPrice { get; set; }

    /// <summary>Gets or sets the discount amount for this item.</summary>
    [PostgresColumn("discount_amount")]
    public decimal DiscountAmount { get; set; }

    /// <summary>Gets or sets the line total.</summary>
    [PostgresColumn("line_total")]
    public decimal LineTotal { get; set; }

    /// <summary>Gets or sets the creation timestamp.</summary>
    [PostgresColumn("created_at")]
    public DateTime CreatedAt { get; set; }

    /// <summary>Gets the total after discount.</summary>
    public decimal TotalAfterDiscount => LineTotal - DiscountAmount;
}

/// <summary>
///     OrderSummary model for reporting and analytics.
///     Demonstrates computed fields and aggregation patterns.
/// </summary>
[PostgresTable("order_summaries")]
public class OrderSummary
{
    /// <summary>Gets or sets the summary identifier.</summary>
    [PostgresColumn("summary_id", PrimaryKey = true)]
    public int SummaryId { get; set; }

    /// <summary>Gets or sets the order identifier.</summary>
    [PostgresColumn("order_id")]
    public int OrderId { get; set; }

    /// <summary>Gets or sets the customer identifier.</summary>
    [PostgresColumn("customer_id")]
    public int CustomerId { get; set; }

    /// <summary>Gets or sets the customer's full name.</summary>
    [PostgresColumn("customer_name")]
    public string CustomerName { get; set; } = string.Empty;

    /// <summary>Gets or sets the order date.</summary>
    [PostgresColumn("order_date")]
    public DateTime OrderDate { get; set; }

    /// <summary>Gets or sets the order status.</summary>
    [PostgresColumn("status")]
    public string Status { get; set; } = string.Empty;

    /// <summary>Gets or sets the total number of items.</summary>
    [PostgresColumn("total_items")]
    public int TotalItems { get; set; }

    /// <summary>Gets or sets the total unique products.</summary>
    [PostgresColumn("total_products")]
    public int TotalProducts { get; set; }

    /// <summary>Gets or sets the subtotal amount.</summary>
    [PostgresColumn("subtotal")]
    public decimal Subtotal { get; set; }

    /// <summary>Gets or sets the total discount amount.</summary>
    [PostgresColumn("total_discount")]
    public decimal TotalDiscount { get; set; }

    /// <summary>Gets or sets the total amount.</summary>
    [PostgresColumn("total_amount")]
    public decimal TotalAmount { get; set; }

    /// <summary>Gets or sets the average item price.</summary>
    [PostgresColumn("avg_item_price")]
    public decimal AverageItemPrice { get; set; }

    /// <summary>Gets or sets the creation timestamp.</summary>
    [PostgresColumn("created_at")]
    public DateTime CreatedAt { get; set; }
}

/// <summary>
///     CheckpointTestRecord model for demonstrating InMemory checkpoint strategy.
/// </summary>
public class CheckpointTestRecord
{
    /// <summary>Gets or sets the record identifier.</summary>
    public int Id { get; set; }

    /// <summary>Gets or sets the record name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the record value.</summary>
    public decimal Value { get; set; }

    /// <summary>Gets or sets the record category.</summary>
    public string Category { get; set; } = string.Empty;

    /// <summary>Gets or sets the creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }
}
