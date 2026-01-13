namespace Sample_LineageExtension;

/// <summary>
///     Represents an order event in the system.
///     This is the primary data type that flows through the pipeline.
/// </summary>
public sealed record OrderEvent
{
    public OrderEvent(
        int orderId,
        int customerId,
        int productId,
        int quantity,
        decimal unitPrice,
        DateTime orderDate,
        OrderStatus status = OrderStatus.Pending,
        string? shippingAddress = null,
        PaymentMethod paymentMethod = PaymentMethod.CreditCard,
        bool isFlaggedForFraud = false)
    {
        OrderId = orderId;
        CustomerId = customerId;
        ProductId = productId;
        Quantity = quantity;
        UnitPrice = unitPrice;
        OrderDate = orderDate;
        Status = status;
        ShippingAddress = shippingAddress;
        PaymentMethod = paymentMethod;
        IsFlaggedForFraud = isFlaggedForFraud;
    }

    /// <summary>
    ///     Gets the unique identifier for the order.
    /// </summary>
    public int OrderId { get; init; }

    /// <summary>
    ///     Gets the customer identifier.
    /// </summary>
    public int CustomerId { get; init; }

    /// <summary>
    ///     Gets the product identifier.
    /// </summary>
    public int ProductId { get; init; }

    /// <summary>
    ///     Gets the quantity ordered.
    /// </summary>
    public int Quantity { get; init; }

    /// <summary>
    ///     Gets the unit price.
    /// </summary>
    public decimal UnitPrice { get; init; }

    /// <summary>
    ///     Gets the total amount (quantity * unit price).
    /// </summary>
    public decimal TotalAmount => Quantity * UnitPrice;

    /// <summary>
    ///     Gets the order date and time.
    /// </summary>
    public DateTime OrderDate { get; init; }

    /// <summary>
    ///     Gets the order status.
    /// </summary>
    public OrderStatus Status { get; init; }

    /// <summary>
    ///     Gets the shipping address.
    /// </summary>
    public string? ShippingAddress { get; init; }

    /// <summary>
    ///     Gets the payment method.
    /// </summary>
    public PaymentMethod PaymentMethod { get; init; }

    /// <summary>
    ///     Gets whether this order is marked for fraud review.
    /// </summary>
    public bool IsFlaggedForFraud { get; init; }

    public override string ToString()
    {
        return $"OrderEvent #{OrderId}: Customer {CustomerId}, Product {ProductId}, Qty {Quantity}, Total {TotalAmount:C}, Status {Status}";
    }
}

/// <summary>
///     Represents customer data used for enrichment.
/// </summary>
public sealed record CustomerData
{
    public CustomerData(
        int customerId,
        string fullName,
        string email,
        string? phone,
        LoyaltyTier loyaltyTier,
        decimal lifetimeValue,
        int orderCount,
        DateTime registrationDate)
    {
        CustomerId = customerId;
        FullName = fullName;
        Email = email;
        Phone = phone;
        LoyaltyTier = loyaltyTier;
        LifetimeValue = lifetimeValue;
        OrderCount = orderCount;
        RegistrationDate = registrationDate;
    }

    /// <summary>
    ///     Gets the unique customer identifier.
    /// </summary>
    public int CustomerId { get; init; }

    /// <summary>
    ///     Gets the customer's full name.
    /// </summary>
    public string FullName { get; init; } = string.Empty;

    /// <summary>
    ///     Gets the customer's email address.
    /// </summary>
    public string Email { get; init; } = string.Empty;

    /// <summary>
    ///     Gets the customer's phone number.
    /// </summary>
    public string? Phone { get; init; }

    /// <summary>
    ///     Gets the customer's loyalty tier.
    /// </summary>
    public LoyaltyTier LoyaltyTier { get; init; }

    /// <summary>
    ///     Gets the total lifetime value of the customer.
    /// </summary>
    public decimal LifetimeValue { get; init; }

    /// <summary>
    ///     Gets the number of orders placed by this customer.
    /// </summary>
    public int OrderCount { get; init; }

    /// <summary>
    ///     Gets the customer's registration date.
    /// </summary>
    public DateTime RegistrationDate { get; init; }

    /// <summary>
    ///     Gets whether the customer is marked as VIP.
    /// </summary>
    public bool IsVip => LoyaltyTier == LoyaltyTier.Platinum || LoyaltyTier == LoyaltyTier.Gold;

    public override string ToString()
    {
        return $"CustomerData #{CustomerId}: {FullName} ({LoyaltyTier}), LTV {LifetimeValue:C}, Orders {OrderCount}";
    }
}

/// <summary>
///     Represents an enriched order with customer information.
///     This is the result of joining order events with customer data.
/// </summary>
public sealed record EnrichedOrder
{
    public EnrichedOrder(
        OrderEvent order,
        CustomerData customer,
        decimal discount,
        ProcessingPriority priority)
    {
        Order = order;
        Customer = customer;
        Discount = discount;
        Priority = priority;
        EnrichedAt = DateTime.UtcNow;
    }

    /// <summary>
    ///     Gets the order event data.
    /// </summary>
    public OrderEvent Order { get; init; } = null!;

    /// <summary>
    ///     Gets the customer data.
    /// </summary>
    public CustomerData Customer { get; init; } = null!;

    /// <summary>
    ///     Gets the calculated discount based on customer loyalty tier.
    /// </summary>
    public decimal Discount { get; init; }

    /// <summary>
    ///     Gets the final amount after discount.
    /// </summary>
    public decimal FinalAmount => Order.TotalAmount - Discount;

    /// <summary>
    ///     Gets the processing priority based on customer tier and order value.
    /// </summary>
    public ProcessingPriority Priority { get; init; }

    /// <summary>
    ///     Gets the enrichment timestamp.
    /// </summary>
    public DateTime EnrichedAt { get; init; }

    public override string ToString()
    {
        return
            $"EnrichedOrder #{Order.OrderId}: {Customer.FullName}, Original {Order.TotalAmount:C}, Discount {Discount:C}, Final {FinalAmount:C}, Priority {Priority}";
    }
}

/// <summary>
///     Represents a validated order ready for processing.
/// </summary>
public sealed record ValidatedOrder
{
    public ValidatedOrder(
        EnrichedOrder enrichedOrder,
        bool isValid,
        IReadOnlyList<string> validationErrors)
    {
        EnrichedOrder = enrichedOrder;
        IsValid = isValid;
        ValidationErrors = validationErrors;
        ValidatedAt = DateTime.UtcNow;
    }

    /// <summary>
    ///     Gets the enriched order.
    /// </summary>
    public EnrichedOrder EnrichedOrder { get; init; } = null!;

    /// <summary>
    ///     Gets whether the order passed validation.
    /// </summary>
    public bool IsValid { get; init; }

    /// <summary>
    ///     Gets the validation errors, if any.
    /// </summary>
    public IReadOnlyList<string> ValidationErrors { get; init; } = Array.Empty<string>();

    /// <summary>
    ///     Gets the validation timestamp.
    /// </summary>
    public DateTime ValidatedAt { get; init; }

    public override string ToString()
    {
        return IsValid
            ? $"ValidatedOrder #{EnrichedOrder.Order.OrderId}: VALID, Final {EnrichedOrder.FinalAmount:C}"
            : $"ValidatedOrder #{EnrichedOrder.Order.OrderId}: INVALID, Errors: {string.Join(", ", ValidationErrors)}";
    }
}

/// <summary>
///     Represents the final processed order ready for storage.
/// </summary>
public sealed record ProcessedOrder
{
    public ProcessedOrder(
        ValidatedOrder validatedOrder,
        ProcessingResult result,
        string? notes = null)
    {
        ValidatedOrder = validatedOrder;
        Result = result;
        Notes = notes;
        ProcessedAt = DateTime.UtcNow;
    }

    /// <summary>
    ///     Gets the validated order.
    /// </summary>
    public ValidatedOrder ValidatedOrder { get; init; } = null!;

    /// <summary>
    ///     Gets the processing result.
    /// </summary>
    public ProcessingResult Result { get; init; }

    /// <summary>
    ///     Gets the processing timestamp.
    /// </summary>
    public DateTime ProcessedAt { get; init; }

    /// <summary>
    ///     Gets any additional processing notes.
    /// </summary>
    public string? Notes { get; init; }

    public override string ToString()
    {
        return $"ProcessedOrder #{ValidatedOrder.EnrichedOrder.Order.OrderId}: Result {Result}, {(Notes != null ? $"Notes: {Notes}" : "")}";
    }
}

/// <summary>
///     Order status enumeration.
/// </summary>
public enum OrderStatus
{
    /// <summary>
    ///     Order is pending processing.
    /// </summary>
    Pending,

    /// <summary>
    ///     Order is being processed.
    /// </summary>
    Processing,

    /// <summary>
    ///     Order has been validated.
    /// </summary>
    Validated,

    /// <summary>
    ///     Order is completed.
    /// </summary>
    Completed,

    /// <summary>
    ///     Order has been cancelled.
    /// </summary>
    Cancelled,

    /// <summary>
    ///     Order has failed processing.
    /// </summary>
    Failed,
}

/// <summary>
///     Payment method enumeration.
/// </summary>
public enum PaymentMethod
{
    /// <summary>
    ///     Credit card payment.
    /// </summary>
    CreditCard,

    /// <summary>
    ///     Debit card payment.
    /// </summary>
    DebitCard,

    /// <summary>
    ///     PayPal payment.
    /// </summary>
    PayPal,

    /// <summary>
    ///     Bank transfer payment.
    /// </summary>
    BankTransfer,

    /// <summary>
    ///     Cash on delivery.
    /// </summary>
    CashOnDelivery,
}

/// <summary>
///     Customer loyalty tier enumeration.
/// </summary>
public enum LoyaltyTier
{
    /// <summary>
    ///     Bronze tier customer.
    /// </summary>
    Bronze,

    /// <summary>
    ///     Silver tier customer.
    /// </summary>
    Silver,

    /// <summary>
    ///     Gold tier customer.
    /// </summary>
    Gold,

    /// <summary>
    ///     Platinum tier customer.
    /// </summary>
    Platinum,
}

/// <summary>
///     Processing priority enumeration.
/// </summary>
public enum ProcessingPriority
{
    /// <summary>
    ///     Low priority processing.
    /// </summary>
    Low,

    /// <summary>
    ///     Normal priority processing.
    /// </summary>
    Normal,

    /// <summary>
    ///     High priority processing.
    /// </summary>
    High,

    /// <summary>
    ///     Urgent priority processing.
    /// </summary>
    Urgent,
}

/// <summary>
///     Processing result enumeration.
/// </summary>
public enum ProcessingResult
{
    /// <summary>
    ///     Order processed successfully.
    /// </summary>
    Success,

    /// <summary>
    ///     Order processing failed.
    /// </summary>
    Failed,

    /// <summary>
    ///     Order was rejected.
    /// </summary>
    Rejected,

    /// <summary>
    ///     Order was sent for manual review.
    /// </summary>
    ManualReview,

    /// <summary>
    ///     Order was sent to dead letter queue.
    /// </summary>
    DeadLetter,
}
