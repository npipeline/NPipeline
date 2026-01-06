namespace Sample_Composition;

/// <summary>
///     Represents a customer record.
/// </summary>
public record Customer(
    int Id,
    string Name,
    string Email,
    string? Phone = null);

/// <summary>
///     Represents a validated customer record.
/// </summary>
public record ValidatedCustomer(
    Customer OriginalCustomer,
    bool IsValid,
    List<string> ValidationErrors);

/// <summary>
///     Represents an enriched customer record.
/// </summary>
public record EnrichedCustomer(
    ValidatedCustomer ValidatedCustomer,
    DateTime EnrichmentTimestamp,
    string? LoyaltyTier = null,
    int LoyaltyPoints = 0);
