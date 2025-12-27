using System;
using NPipeline.DataFlow;

namespace Sample_KeyedJoinNode;

/// <summary>
///     Represents a customer order with basic information.
/// </summary>
/// <param name="OrderId">Unique identifier for the order.</param>
/// <param name="CustomerId">Identifier of the customer who placed the order.</param>
/// <param name="ProductCode">Product code that was ordered.</param>
/// <param name="Quantity">Quantity of the product ordered.</param>
/// <param name="UnitPrice">Unit price of the product.</param>
/// <param name="OrderDate">Date when the order was placed.</param>
public sealed record Order(
    int OrderId,
    int CustomerId,
    string ProductCode,
    int Quantity,
    decimal UnitPrice,
    DateTime OrderDate
)
{
    /// <summary>
    ///     Gets the total price of the order (Quantity * UnitPrice).
    /// </summary>
    public decimal TotalPrice => Quantity * UnitPrice;
}

/// <summary>
///     Represents customer information.
/// </summary>
/// <param name="CustomerId">Unique identifier for the customer.</param>
/// <param name="Name">Customer's full name.</param>
/// <param name="Email">Customer's email address.</param>
/// <param name="RegistrationDate">Date when the customer registered.</param>
/// <param name="CustomerTier">Customer tier (Bronze, Silver, Gold, Platinum).</param>
public sealed record Customer(
    int CustomerId,
    string Name,
    string Email,
    DateTime RegistrationDate,
    string CustomerTier
);

/// <summary>
///     Represents product information.
/// </summary>
/// <param name="ProductCode">Unique product code.</param>
/// <param name="ProductName">Name of the product.</param>
/// <param name="Category">Product category.</param>
/// <param name="Price">Standard price of the product.</param>
public sealed record Product(
    string ProductCode,
    string ProductName,
    string Category,
    decimal Price
);

/// <summary>
///     Represents the result of joining an order with customer information.
/// </summary>
/// <param name="Order">The order information.</param>
/// <param name="Customer">The customer information.</param>
public sealed record OrderCustomerJoin(
    Order Order,
    Customer Customer
)
{
    /// <summary>
    ///     Gets the customer ID (convenient access).
    /// </summary>
    public int CustomerId => Order.CustomerId;

    /// <summary>
    ///     Gets the order ID (convenient access).
    /// </summary>
    public int OrderId => Order.OrderId;

    /// <summary>
    ///     Gets the customer name (convenient access).
    /// </summary>
    public string CustomerName => Customer.Name;

    /// <summary>
    ///     Gets the customer tier (convenient access).
    /// </summary>
    public string CustomerTier => Customer.CustomerTier;
}

/// <summary>
///     Represents an enriched order with both customer and product information.
/// </summary>
/// <param name="OrderCustomerJoin">The order-customer join information.</param>
/// <param name="Product">The product information.</param>
public sealed record EnrichedOrder(
    OrderCustomerJoin OrderCustomerJoin,
    Product Product
) : ITimestamped
{
    /// <summary>
    ///     Gets the order information (convenient access).
    /// </summary>
    public Order Order => OrderCustomerJoin.Order;

    /// <summary>
    ///     Gets the customer information (convenient access).
    /// </summary>
    public Customer Customer => OrderCustomerJoin.Customer;

    /// <summary>
    ///     Gets the order ID (convenient access).
    /// </summary>
    public int OrderId => Order.OrderId;

    /// <summary>
    ///     Gets the customer name (convenient access).
    /// </summary>
    public string CustomerName => Customer.Name;

    /// <summary>
    ///     Gets the product name (convenient access).
    /// </summary>
    public string ProductName => Product.ProductName;

    /// <summary>
    ///     Gets the product category (convenient access).
    /// </summary>
    public string ProductCategory => Product.Category;

    /// <summary>
    ///     Gets the customer tier (convenient access).
    /// </summary>
    public string CustomerTier => Customer.CustomerTier;

    /// <summary>
    ///     Gets the timestamp for this enriched order (using the order date).
    /// </summary>
    public DateTimeOffset Timestamp => Order.OrderDate;
}

/// <summary>
///     Represents a sales summary by customer tier.
/// </summary>
/// <param name="CustomerTier">The customer tier.</param>
/// <param name="TotalOrders">Total number of orders.</param>
/// <param name="TotalRevenue">Total revenue from all orders.</param>
/// <param name="AverageOrderValue">Average order value.</param>
/// <param name="UniqueCustomers">Number of unique customers.</param>
public sealed record SalesByCustomerTier(
    string CustomerTier,
    int TotalOrders,
    decimal TotalRevenue,
    decimal AverageOrderValue,
    int UniqueCustomers
);

/// <summary>
///     Represents a sales summary by product category.
/// </summary>
/// <param name="Category">The product category.</param>
/// <param name="TotalOrders">Total number of orders.</param>
/// <param name="TotalQuantity">Total quantity sold.</param>
/// <param name="TotalRevenue">Total revenue from all orders.</param>
/// <param name="TopProduct">Top selling product in this category.</param>
public sealed record SalesByCategory(
    string Category,
    int TotalOrders,
    int TotalQuantity,
    decimal TotalRevenue,
    string TopProduct
);
